# Trajectory Tracking — Implementation Plan

## Context

The platform today keeps **only the latest state per vessel**: the streaming topology stores one `VesselState` per MMSI in a RocksDB `KeyValueStore` (`MaritimeTopology` line 177), and the hot tier (`vessel_risk`, `schema.sql`) is a single upserted row per MMSI (`PostgresVesselStateStore` line 35, `ON CONFLICT (mmsi) DO UPDATE`). The full position history exists **only** as the immutable Parquet cold tier (`FileSystemParquetColdTier`), which is laid out for *batch* scans, not interactive queries.

What's missing is the **track** — the connected, time-ordered path a vessel has travelled, which is the foundational primitive of any maritime-intelligence product (replay, voyage analysis, port calls, rendezvous detection). This plan adds trajectory tracking as a first-class capability without disturbing the existing speed/batch/serving layers.

**Design philosophy (consistent with the existing Lambda architecture):**
- **Warm store** for interactive track queries: append every position to a PostGIS-indexed `vessel_positions` table — fast `WHERE mmsi = ? AND ts BETWEEN ? AND ?` range scans the cold Parquet tier can't serve cheaply.
- **Batch layer** for derived intelligence: a Spark job reconstructs voyages and port calls over the complete cold tier.
- **Serving layer**: API-service endpoints return **GeoJSON**, the lingua franca of every mapping client (Leaflet/Mapbox/OpenLayers), so the output is demoable on a map with zero translation.

**Confirmed constraints (from the codebase):**
- Avro + Schema Registry for all Kafka payloads; `EnrichedVesselEvent` carries the nested `VesselEvent` (`mmsi`, `latitude`, `longitude`, `speed`, `timestamp`, …).
- PostGIS is already the geospatial authority (`ZoneRepository`, `ST_Contains`); reuse it, don't add a second geo engine. JTS is already on the classpath (`GeoUtils`) for in-JVM geometry.
- Storage depends only on the `ColdTierWriter` / `VesselStateStore` ports — new persistence goes behind a new port in the same style.
- **Invariant:** the repo must compile and `./mvnw verify` must stay green after every phase.

**Known caveat to fix along the way:** `FileSystemParquetColdTier` line 56 partitions by `LocalDate.now()` (ingest date), **not** the event timestamp. Trajectory reconstruction must order by event `timestamp`, and the batch voyage job must not assume the partition date equals the event date. Phase T4 addresses this explicitly.

---

## Phased plan

Order: **T1 → T2 → T3 → T4 → T5**. Each phase is independently shippable and ends with a verification step. T1–T2 deliver the core trajectory feature; T3–T5 build the higher-value intelligence on top.

### Phase T1 — Trajectory store (warm tier: `vessel_positions`)

- **Proves:** modelling time-series geospatial data at scale; choosing the right store for the access pattern.
- **Why (teach):** the hot tier answers "where is vessel X *now*"; the cold tier answers "aggregate everything" via batch. Neither answers "give me vessel X's path between 09:00 and 12:00" interactively. That's a **range scan on (mmsi, ts)** with a geospatial payload — exactly what an indexed Postgres/PostGIS table is built for. Appending here (vs. upserting like `vessel_risk`) is the key semantic shift: we *accumulate* history instead of overwriting it.
- **Decision:** new `vessel_positions` table with a PostGIS `geometry(Point,4326)` column and a composite PK `(mmsi, ts)`. A new storage port `PositionHistoryStore` (mirroring `VesselStateStore`) keeps the controller decoupled from the backing store. Writes happen on the **`maritime.enriched`** path only (every event), not `maritime.detections` (which is a flagged subset — would create gaps in the track).
- **Create:**
  - Migration `maritime-enricher/.../db/migration/V3__vessel_positions.sql`:
    ```sql
    CREATE TABLE IF NOT EXISTS vessel_positions (
        mmsi      VARCHAR(9)               NOT NULL,
        ts        TIMESTAMPTZ              NOT NULL,
        lat       DOUBLE PRECISION         NOT NULL,
        lon       DOUBLE PRECISION         NOT NULL,
        speed_kn  DOUBLE PRECISION,
        heading   DOUBLE PRECISION,
        geom      geometry(Point, 4326)    NOT NULL,
        PRIMARY KEY (mmsi, ts)
    );
    -- Range-scan a vessel's track in time order (covers the API query).
    CREATE INDEX IF NOT EXISTS vessel_positions_mmsi_ts_idx
        ON vessel_positions (mmsi, ts);
    -- Spatial index for bbox / proximity queries (Phase T5).
    CREATE INDEX IF NOT EXISTS vessel_positions_geom_idx
        ON vessel_positions USING GIST (geom);
    ```
  - `PositionHistoryStore` interface + `PostgresPositionHistoryStore` impl in `maritime-storage/.../service/`. `append(EnrichedVesselEvent)` does an idempotent insert: `INSERT ... ON CONFLICT (mmsi, ts) DO NOTHING` (at-least-once delivery means the same position can arrive twice — dedupe at the sink, same principle as `DedupService`). Populate `geom` via `ST_SetSRID(ST_MakePoint(?, ?), 4326)`.
  - Wire `positionHistory.append(event)` into `VesselController.persist()` (line 71) alongside the existing cold/hot writes. Order: cold → position-history → hot, then ack (offset still commits only after all sinks succeed).
- **Verify:** run the simulator; `SELECT count(*) FROM vessel_positions WHERE mmsi = '...'` grows monotonically (not capped at 1 like `vessel_risk`); duplicate replay of a record does not create a second row.

### Phase T2 — Track reconstruction API (GeoJSON)

- **Proves:** turning stored points into a usable domain object; serving a standards-based geospatial contract.
- **Why (teach):** a list of `(lat, lon, ts)` rows isn't a *track* — a track is an **ordered** geometry. Emitting GeoJSON (a `LineString` for the path plus optional `Point` features for per-fix detail) means any web map renders it directly. Ordering by `ts` and capping the result is what separates a correct API from one that OOMs on a chatty vessel.
- **Decision:** a new read path on the storage service exposed through the API-service serving layer, consistent with the existing `/{mmsi}` and `/{mmsi}/history` split. Build the geometry with JTS (`GeoUtils` already wraps it) and serialize to GeoJSON. Bound the response with a default and max point cap; support optional `from`/`to` time filters and a `simplify` toggle (Douglas–Peucker via JTS `TopologyPreservingSimplifier`) so large tracks stay light.
- **Create:**
  - Storage: `GET /api/v1/vessels/{mmsi}/track?from=&to=&limit=` → reads `vessel_positions` ordered by `ts ASC`, returns a GeoJSON `FeatureCollection` (one `LineString` Feature for the path; properties carry `mmsi`, `pointCount`, `from`, `to`). Add a `Track`/`TrackPoint` view model in `maritime-storage`.
  - API service: `GET /api/v1/intelligence/{mmsi}/track` proxying storage (same pattern as `getVesselIntelligence`, `ApiController` line 56), so the public contract stays API-service-fronted.
  - Document the GeoJSON shape in the endpoint Javadoc (as `/{mmsi}/history` already does).
- **Verify:** `curl :8084/api/v1/intelligence/{mmsi}/track` returns valid GeoJSON (paste into geojson.io → the simulator's Gulf-Coast track is visible); `from`/`to` narrows the line; an unknown MMSI returns 404; the loiterer's track shows as a tight cluster, the normal vessel's as a line.

### Phase T3 — Near-real-time recent track (speed layer)

- **Proves:** bounded stateful stream processing; choosing in-memory vs. queried history per latency need.
- **Why (teach):** Phase T2 answers historical queries from Postgres, but a live map wants the *last few minutes* with sub-second freshness, without a DB round-trip per refresh. The Streams state store already holds per-MMSI state in RocksDB; extending `VesselState` with a **bounded ring buffer** of recent fixes (e.g. last N=50 or last 15 min) gives an O(1) "recent track" served straight from the topology's interactive queries — and it's the substrate the future rendezvous detector needs (it must compare *recent* paths of two vessels).
- **Decision:** extend `VesselState` (and `VesselStateSerdes`) with a fixed-capacity deque of `(ts, lat, lon)`; evict by count and age in `process()` (`MaritimeTopology` line 223). Expose via Kafka Streams **interactive queries** (`KafkaStreams.store(...)`) behind a small read endpoint on the enricher service. This deliberately exercises the same backward-compatible-state concern that Avro field additions did — the changelog topic must tolerate the widened state.
- **Create:**
  - Widen `VesselState` + serde; enforce the bound on every `process()` call.
  - Enricher read endpoint `GET /api/v1/vessels/{mmsi}/recent-track` using interactive queries; returns the same GeoJSON shape as T2 for client symmetry.
  - Metric: gauge for state-store size / evictions (Micrometer, consistent with the existing `detections` counters at line 248).
- **Verify:** while the simulator runs, the recent-track endpoint reflects new positions within one poll interval; restart enricher → the buffer rehydrates from the changelog (state survives, as the loitering/dark detectors already do); buffer never exceeds the cap.

### Phase T4 — Voyage segmentation & port-call detection (batch layer)

- **Proves:** deriving domain events from raw tracks; exact batch computation over the immutable tier (Lambda batch layer, alongside the existing Spark jobs).
- **Why (teach):** a continuous stream of fixes becomes *intelligence* once segmented into **voyages** (port A → port B) and **port calls** (arrival/dwell/departure). The segmentation rules reuse signals already in the system: a long reporting **gap** (the dark-vessel threshold, `DARK_VESSEL_MINUTES`) or a sustained **low-speed dwell inside a PORT zone** (`ZoneRepository`, `LOITER_SPEED_KN`) ends one voyage and begins the next. Doing this in Spark — not streams — is the right call: it needs the *complete* ordered history per vessel and exact boundaries, which is a batch window-function problem (`lag`/`lead` over `Window.partitionBy(mmsi).orderBy(ts)`), not a per-event one.
- **Decision:** new `maritime-spark` job `VoyageSegmentationJob` reading the cold Parquet tier (same `SparkSessionFactory` / `JobConfig` as `DailyVesselAggregatesJob`), writing two PostGIS tables read by the API service. **Fix the partition-date caveat:** order and segment by the event `timestamp` from `vesselEvent.timestamp`, never by the `date=` partition column (which is ingest date — see `FileSystemParquetColdTier` line 56). Port association uses a Spark→PostGIS lookup or a broadcast of the (small) zones catalog.
- **Create:**
  - Migration `V4__voyages.sql`:
    ```sql
    CREATE TABLE IF NOT EXISTS voyages (
        mmsi         VARCHAR(9)   NOT NULL,
        voyage_id    VARCHAR(64)  NOT NULL,   -- deterministic: mmsi + start ts
        start_ts     TIMESTAMPTZ  NOT NULL,
        end_ts       TIMESTAMPTZ  NOT NULL,
        start_zone   VARCHAR(128),            -- PORT zone name or null (open sea)
        end_zone     VARCHAR(128),
        distance_nm  DOUBLE PRECISION,        -- summed Haversine over the leg
        duration_min BIGINT,
        point_count  BIGINT,
        PRIMARY KEY (mmsi, voyage_id)
    );
    CREATE TABLE IF NOT EXISTS port_calls (
        mmsi        VARCHAR(9)   NOT NULL,
        port_zone   VARCHAR(128) NOT NULL,
        arrival_ts  TIMESTAMPTZ  NOT NULL,
        departure_ts TIMESTAMPTZ,             -- null while still in port
        dwell_min   BIGINT,
        PRIMARY KEY (mmsi, port_zone, arrival_ts)
    );
    ```
  - `VoyageSegmentationJob`: window functions to compute inter-fix gaps and zone transitions, label voyage boundaries, sum leg distance (reuse the Haversine math from `GeoUtils`/the speed-anomaly detector), and emit port-call arrival/departure pairs.
  - Unit test in `SparkJobsTest` style: synthetic Parquet (a vessel that leaves a PORT zone, transits, returns) written to `@TempDir`, output asserted against H2 — covering boundary-by-gap and boundary-by-port-dwell.
- **Verify:** `spark-submit` (or `mvn exec:java -Plocal`) populates `voyages` and `port_calls`; a vessel that loiters in a PORT zone then departs yields one closed port call + a new voyage; the dark vessel's silence splits its track into two voyages.

### Phase T5 — Serving & visualization

- **Proves:** assembling the pieces into a coherent, demoable product surface.
- **Why (teach):** the value of tracks, voyages, and port calls is only legible when a client can fetch and render them together. A bounding-box "what's on screen" query is the standard live-map backend and the natural home for the `vessel_positions` GiST index from T1.
- **Decision:** API-service endpoints only (serving layer); no new storage logic beyond thin reads. All geo responses are GeoJSON for client uniformity.
- **Create:**
  - API service: `GET /api/v1/intelligence/{mmsi}/voyages` and `/{mmsi}/port-calls` (JDBC reads of the T4 tables, ordered desc, capped — same shape as the existing `/{mmsi}/history` query at `ApiController` line 102).
  - API service: `GET /api/v1/intelligence/vessels?bbox=minLon,minLat,maxLon,maxLat` → latest position per vessel within the box (PostGIS `ST_MakeEnvelope` against `vessel_positions`/`vessel_risk`), returned as a GeoJSON `FeatureCollection` of points — the live-map feed.
  - Optional (stretch): a minimal static `map.html` served by the API service using Leaflet to plot a track + voyages, turning the project from "logs in a console" into a visible demo.
- **Verify:** the bbox endpoint returns only vessels inside the envelope; `/voyages` and `/port-calls` return the T4 rows; the optional map renders a vessel's track with voyage segments.

---

## Cross-cutting concerns

- **Observability:** add Micrometer counters/timers for position appends and track-query latency (mirror the `detections` counters and existing latency timers); a Grafana panel for positions/sec and `vessel_positions` row growth.
- **Retention / growth:** `vessel_positions` grows unbounded. Note (and, if time allows, implement) a strategy — PostgreSQL native range partitioning by day on `ts`, or a scheduled purge of rows older than the cold-tier retention window. Surface this as an explicit decision rather than letting the table grow silently.
- **Tests:** unit-test the GeoJSON serialization (track ordering, empty track → empty/200, cap enforcement) and the voyage-segmentation boundary logic; an integration test (Testcontainers Postgres, in the `StorageIntegrationIT` style) asserting append + range-query round-trips through PostGIS.
- **Schema evolution:** widening `VesselState` (T3) must remain readable by an already-running topology — exercise the same backward-compatibility discipline the Avro additions used.

## Verification (overall)

- After **every** phase: `./mvnw clean verify` compiles and passes tests.
- End-to-end smoke: `make up` → run the four services → `POST :8081/api/v1/simulate/start` → `vessel_positions` fills → `GET :8084/api/v1/intelligence/{mmsi}/track` returns GeoJSON that renders as the simulator's tracks (normal vessel = line, loiterer = cluster) → run `VoyageSegmentationJob` → `/voyages` and `/port-calls` return derived events.
- Phase-specific checks: idempotent position append (no dupes on replay); recent-track survives an enricher restart; voyage boundaries fall on dark-vessel gaps and port dwells; bbox query respects the envelope.
