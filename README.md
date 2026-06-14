# Maritime Intelligence Platform - Learning Project

This project simulates a simplified version of the Maritime Intelligence Platform described in the job description. It demonstrates the core architecture using Java, Spring Boot, Kafka, and Postgres — and runs entirely on localhost with no cloud dependency.

## What This Project Focuses On

This is a **senior-level backend work sample**, not a tutorial. Its focus is showing *how* a production maritime data pipeline is engineered, with the rationale behind each choice. Concretely, it is focused on:

-   **Real-time streaming pipelines** — an end-to-end Kafka pipeline (ingest → enrich → detect → store → serve) with explicit topics, consumer groups, and partitioning by vessel.
-   **Stateful stream processing** — per-vessel behavioral detection (loitering, speed anomaly, dark vessel) on fault-tolerant Kafka Streams state, distinct from the stateless ETL path.
-   **Data integrity & quality** — schema-on-write with Avro + Schema Registry, validation with a quarantine path, consumer-side deduplication, and at-least-once delivery with dead-letter handling.
-   **The maritime domain** — geospatial enrichment against real PostGIS geofences (restricted / port / EEZ zones), risk scoring, and AIS-specific signals.
-   **Lambda architecture** — a real-time *speed layer* (Kafka Streams) and an exact *batch layer* (Spark over a Parquet cold tier), merged behind one *serving layer* (the API service).
-   **Tiered storage** — a Postgres hot tier for low-latency queries and a partitioned Parquet cold tier for batch analytics, decoupled behind swappable ports.
-   **Operability** — observability (Micrometer + Prometheus + Grafana), correlation-ID tracing across async hops, structured logging, and CI.
-   **Design rationale** — every significant decision (plain Kafka vs. Kafka Streams, separate detections topic, ack-after-side-effect, schema evolution) is documented with the *why*, not just the *what*.

> **Non-goals:** this is a localhost learning/portfolio build — it does **not** target cloud deployment, authentication/authorization, horizontal-scale tuning, or a production AIS feed. Those are deliberately out of scope so the focus stays on the data-pipeline engineering above. Planned next steps (vessel trajectory tracking) are tracked in [`PLAN.md`](NEXT_FEATURES.md).

## Architecture Overview

1.  **Ingestion Service**: Simulates AIS vessel data feeds and pushes them to a Kafka topic (`maritime.ais.raw`).
2.  **Enricher Service**: Consumes raw vessel events from Kafka, enriches them with geospatial data (checking against restricted zones), calculates a risk score, and pushes the enriched data to another Kafka topic (`maritime.enriched`).
3.  **Storage Service**: Consumes enriched events from Kafka, saves "hot" data (latest state per vessel) to Postgres and "cold" data to a partitioned Parquet archive on the local filesystem. Provides a REST API to query vessel risk data. The storage tiers sit behind `VesselStateStore` / `ColdTierWriter` interfaces, so the backing store is swappable.
4.  **API Service**: Aggregates data from the Storage Service and provides a unified API endpoint for frontend consumption.

## Modules

The project is a Maven multi-module build with **six modules** — four runnable Spring Boot services, one shared library, and one standalone Spark application.

| Module | Type | Port | Responsibility |
|---|---|---|---|
| `maritime-common` | Library (shared) | — | Cross-cutting code every service depends on. |
| `maritime-ingestion` | Spring Boot service | 8081 | Source of events — the AIS simulator. |
| `maritime-enricher` | Spring Boot service | 8082 | Enrichment, risk scoring, and stateful detection. |
| `maritime-storage` | Spring Boot service | 8083 | Tiered persistence + real-time query API. |
| `maritime-api` | Spring Boot service | 8084 | Public-facing serving layer / API aggregation. |
| `maritime-spark` | Standalone Spark app | — | Batch analytics over the cold tier (`spark-submit`). |

### `maritime-common` — shared library
The dependency every service pulls in. Holds the **Avro-generated DTOs** (`VesselEvent`, `EnrichedVesselEvent`) so all services share one schema; **validation** (`VesselEventValidator`, `ValidationResult`); **geospatial math** (`GeoUtils` — Haversine + JTS point-in-polygon); the **Avro-JSON serde** (`AvroJson`) used for the HTTP round-trip; and **observability plumbing** (`CorrelationIds`, the HTTP filter / Kafka interceptors, and an `ObservabilityAutoConfiguration` auto-wired into each service). Has no `main` — it is a JAR, not a service.

### `maritime-ingestion` (8081) — event source
The start of the pipeline. `AisSimulatorController` drives a four-vessel demo fleet on a scheduler, each vessel shaped to trip one detector, and produces `VesselEvent`s to `maritime.ais.raw` keyed by MMSI. Exposes `POST /api/v1/simulate/start|stop`. In a real deployment this module is where a live AIS feed adapter would sit.

### `maritime-enricher` (8082) — the brains
Two distinct processing styles live here (see the design-rationale note below):
-   **Stateless ETL** — `RiskScorerService` (`@KafkaListener` + `KafkaTemplate`): validate → dedup → PostGIS zone enrichment → risk score → publish to `maritime.enriched`; bad/duplicate events go to `maritime.ais.quarantine`.
-   **Stateful detection** — `MaritimeTopology` (Kafka Streams): per-MMSI RocksDB state driving the loitering / speed-anomaly / dark-vessel detectors, emitting flagged events to `maritime.detections`.
Also owns the shared database's Flyway migrations (zones catalog + Spark output tables).

### `maritime-storage` (8083) — persistence + query
Consumes `maritime.enriched` **and** `maritime.detections`. Writes each event to a **hot tier** (`PostgresVesselStateStore` — latest state per MMSI) and a **cold tier** (`FileSystemParquetColdTier` — partitioned Parquet archive), both behind the `VesselStateStore` / `ColdTierWriter` ports so the backing store is swappable. Serves `GET /api/v1/vessels/{mmsi}` for real-time state.

### `maritime-api` (8084) — serving layer
The public API. Proxies real-time state from storage (`GET /api/v1/intelligence/{mmsi}`) and reads the Spark batch tables in Postgres directly for history (`/{mmsi}/history`). It is the Lambda-architecture *serving layer* that merges the speed-layer and batch-layer views behind one contract.

### `maritime-spark` — batch layer
A standalone Spark application (not a Spring service), deliberately isolated so its heavy dependency tree never collides with the services' classpaths. Three jobs (`DailyVesselAggregatesJob`, `RiskRollupJob`, `LoiteringHotspotJob`) read the Parquet cold tier and write rollups to Postgres, run via `spark-submit` or `mvn exec:java -Plocal`.

## Implemented Features

The following are built and working today. (Planned trajectory-tracking work is tracked separately in [`PLAN.md`](NEXT_FEATURES.md).)

### Ingestion & simulation
-   **AIS simulator** (`AisSimulatorController`) drives a four-vessel demo fleet on a 2-second scheduler, each vessel exercising a specific detector: a *normal* vessel (steady track), a *loiterer* (drifting at ~0.3 kn), a *dark vessel* (stops emitting mid-run), and a *speed-anomaly* vessel (position jumps inconsistent with reported speed).
-   Start/stop via `POST /api/v1/simulate/start` and `/stop`.

### Streaming, enrichment & risk scoring
-   **ETL pipeline** (`RiskScorerService`): Validate → Dedup → Enrich → Score, consuming `maritime.ais.raw` and producing `maritime.enriched`.
-   **Validation** (`VesselEventValidator`): MMSI format (9 digits), lat/lon bounds, null-island `(0,0)` rejection, timestamp staleness, and a generous speed ceiling. Bad records are routed to `maritime.ais.quarantine` with a reason header rather than crashing the pipeline.
-   **Deduplication** (`DedupService`): Caffeine-backed, keyed on `(mmsi, timestamp)` with TTL expiry — the consumer-side idempotency that at-least-once delivery requires.
-   **Geospatial enrichment** (`ZoneRepository`): PostGIS `ST_Contains` lookup against a zones catalog (RESTRICTED / PORT / EEZ) with a GiST index.
-   **Risk scoring**: additive model over zone type, near-port proximity, and speed, mapped to LOW / MEDIUM / HIGH.

### Stateful behavioral detection (speed layer)
-   **Kafka Streams topology** (`MaritimeTopology`) with per-MMSI state in a RocksDB-backed store (fault-tolerant via changelog). Three detectors:
    -   *Loitering* — sustained low-speed dwell.
    -   *Speed anomaly* — Haversine-implied speed vs. reported SOG divergence.
    -   *Dark vessel* — wall-clock punctuator scanning for reporting gaps (silence).
-   Detections are emitted to a dedicated `maritime.detections` topic (never fed back into the input), keeping the data flow a strict DAG.

### Tiered storage
-   **Hot tier** (`PostgresVesselStateStore`): latest state per vessel, `INSERT … ON CONFLICT (mmsi) DO UPDATE` upsert, with a canonical Avro-JSON `payload` column plus flat queryable columns.
-   **Cold tier** (`FileSystemParquetColdTier`): Snappy-compressed Parquet under a Hive-style `date=/mmsi=` partition layout for Spark partition pruning.
-   Both sit behind `VesselStateStore` / `ColdTierWriter` ports, so the backing store is swappable. The storage service consumes both `maritime.enriched` and `maritime.detections`, acking only after both tier writes succeed.

### Batch analytics (Spark — batch layer)
-   **`DailyVesselAggregatesJob`**: per-vessel daily event counts, avg/max speed, avg risk, and detection-flag counts → `vessel_daily_stats`.
-   **`RiskRollupJob`**: p50/p95 risk percentiles per vessel per day → `vessel_risk_percentiles`.
-   **`LoiteringHotspotJob`**: top-N loitering grid cells → `loitering_hotspots` (PostGIS GiST-indexed).
-   Jobs read the cold Parquet tier via a shared `SparkSessionFactory` / `JobConfig`, writing idempotently to PostGIS.

### Serving layer (API service)
-   `GET /api/v1/intelligence/{mmsi}` — latest real-time enriched state (speed layer, via the storage hot tier).
-   `GET /api/v1/intelligence/{mmsi}/history` — Spark-computed daily history merging `vessel_daily_stats` + `vessel_risk_percentiles` (batch layer), capped at 90 days.

### Data contracts
-   **Avro + Confluent Schema Registry** for all Kafka payloads (`VesselEvent`, `EnrichedVesselEvent`), with backward-compatible field evolution (union-with-`null`, defaulted detection flags).

### Observability
-   **Micrometer + Prometheus + Grafana**: per-detection counters and latency timers; a provisioned Grafana dashboard.
-   **Correlation IDs** propagated across async hops via Kafka headers and HTTP, bound to the logging MDC for end-to-end tracing of a single event across all services.

### Testing & build
-   **Unit tests** for validation, geo math, and dedup (including a 20-thread race test).
-   **Integration tests** via Testcontainers: the enricher pipeline (real Kafka + Schema Registry) and storage (real Postgres); Spark jobs tested against an H2 in-memory DB.
-   **CI** (`./mvnw verify` on JDK 17), a `Makefile` for common workflows, and `docker-compose` for the full local stack (Kafka, Zookeeper, Schema Registry, PostGIS, Prometheus, Grafana).

## Event Flow — the life of a vessel event

A single AIS report travels through all four services and up to four Kafka topics. Every Kafka payload is Avro, keyed by `mmsi` (so a vessel's events stay ordered on one partition). Each consumer commits its offset **only after** its side effect succeeds (ack-after-side-effect, at-least-once).

### Topics & consumer groups

| Topic | Produced by | Consumed by (group) | Payload |
|---|---|---|---|
| `maritime.ais.raw` | Ingestion (`AisSimulatorController`) | Enricher `RiskScorerService` (`enricher-service`) | `VesselEvent` |
| `maritime.enriched` | Enricher `RiskScorerService` | Enricher `MaritimeTopology` (`maritime-detection-topology`) **and** Storage `VesselController` (`storage-service`) | `EnrichedVesselEvent` |
| `maritime.detections` | Enricher `MaritimeTopology` | Storage `VesselController` (`storage-service`) | `EnrichedVesselEvent` (≥1 flag set) |
| `maritime.ais.quarantine` | Enricher `RiskScorerService` | (audit sink) | `VesselEvent` + `reason` header |
| `maritime.ais.raw.DLT` / `maritime.enriched.DLT` | `DefaultErrorHandler` after retries exhausted | (dead-letter sink) | original record |

> **Two independent consumers of `maritime.enriched`.** The detection topology and the storage service each read every enriched event under their *own* group, so both receive the full stream. The topology re-publishes only *flagged* events to the separate `maritime.detections` topic — it never writes back to `maritime.enriched`, which keeps the flow a strict DAG (no feedback loop).

### Sequence

```mermaid
sequenceDiagram
    participant SIM as Ingestion<br/>(AIS Simulator)
    participant RAW as maritime.ais.raw
    participant RSS as Enricher<br/>(RiskScorerService)
    participant Q as maritime.ais.quarantine
    participant ENR as maritime.enriched
    participant TOP as Enricher<br/>(MaritimeTopology)
    participant DET as maritime.detections
    participant STO as Storage<br/>(VesselController)
    participant DB as Postgres hot tier<br/>+ Parquet cold tier
    participant GW as API service

    SIM->>RAW: produce VesselEvent (key=mmsi)
    RAW->>RSS: consume (group: enricher-service)
    Note over RSS: Validate → Dedup → Enrich (PostGIS zone, risk score)
    alt invalid or duplicate
        RSS->>Q: quarantine + reason header, then ack
    else valid
        RSS->>ENR: produce EnrichedVesselEvent, ack after send
    end

    ENR->>TOP: consume (group: maritime-detection-topology)
    Note over TOP: RocksDB per-MMSI state<br/>loitering / speedAnomaly / darkVessel
    TOP->>DET: produce only if a flag fired

    ENR->>STO: consume (group: storage-service)
    DET->>STO: consume (group: storage-service)
    STO->>DB: cold-tier write → hot-tier upsert, then ack

    GW->>STO: GET /vessels/{mmsi} (real-time)
    Note over GW: GET /{mmsi}/history reads Spark<br/>batch tables in Postgres directly
```

### Step by step

1. **Ingest** — the simulator emits a `VesselEvent` to `maritime.ais.raw`, keyed by MMSI, stamping a correlation ID into the MDC and Kafka headers.
2. **Validate & dedup** — `RiskScorerService` (`enricher-service` group) validates the event and checks the Caffeine dedup cache. Invalid or duplicate events go to `maritime.ais.quarantine` with a `reason` header; the offset is acked only after the quarantine send confirms.
3. **Enrich & score** — valid events get a PostGIS zone lookup and a risk score, are wrapped as an `EnrichedVesselEvent` (detection flags default `false`), and published to `maritime.enriched`. Offset acked only after the produce callback succeeds.
4. **Detect (speed layer)** — `MaritimeTopology` (`maritime-detection-topology` group) consumes `maritime.enriched`, updates per-MMSI RocksDB state, and runs the loitering / speed-anomaly / dark-vessel detectors. Events where a flag fires are re-published to `maritime.detections`; clean events produce no output.
5. **Persist** — `VesselController` (`storage-service` group) subscribes to **both** `maritime.enriched` and `maritime.detections`. Each event is written to the Parquet cold tier and upserted into the Postgres hot tier, then acked.
6. **Serve** — the API service exposes `GET /api/v1/intelligence/{mmsi}` (real-time, proxied from the storage hot tier) and `/{mmsi}/history` (Spark batch rollups read straight from Postgres).
7. **Failure path** — if a consumer throws, `DefaultErrorHandler` retries (fixed backoff, 3 attempts); exhausted records land in `<topic>.DLT` instead of blocking the partition.

## Prerequisites

-   Java 17+
-   Maven 3.8+
-   Docker & Docker Compose

## How to Run

### 1. Start Infrastructure
```bash
docker-compose up -d
```
This starts Kafka, Zookeeper, Schema Registry, and PostgreSQL.

### 2. Build the Project
```bash
mvn clean install -DskipTests
```

### 3. Run Services
Run each service in a separate terminal:

```bash
# Ingestion Service (Port 8081)
cd maritime-ingestion && mvn spring-boot:run

# Enricher Service (Port 8082)
cd ../maritime-enricher && mvn spring-boot:run

# Storage Service (Port 8083)
cd ../maritime-storage && mvn spring-boot:run

# API Service (Port 8084)
cd ../maritime-api && mvn spring-boot:run
```

### 4. Trigger Data Simulation
Start the AIS simulator:
```bash
curl -X POST http://localhost:8081/api/v1/simulate/start
```

You should see logs in the Enricher and Storage service consoles indicating data processing.

### 5. Query Data
Retrieve vessel intelligence via the API service:
```bash
curl http://localhost:8084/api/v1/intelligence/123456789
```

### 6. Stop Simulation
```bash
curl -X POST http://localhost:8081/api/v1/simulate/stop
```

## Key Concepts Demonstrated

-   **Maven Multi-Module Project**: Shared code in `maritime-common`.
-   **Kafka Streaming**: Producer/Consumer pattern with topic partitioning.
-   **Geospatial Processing**: Point-in-polygon checks using JTS.
-   **Tiered Storage**: Postgres hot tier (latest state per vessel) + local Parquet cold tier, decoupled behind storage-port interfaces.
-   **Microservices Communication**: REST API aggregation via the API service.

## Key Engineering Decisions

The platform follows a **Lambda architecture**: a real-time *speed layer* (Kafka Streams), a *batch layer* (Spark jobs computing exact daily aggregates and percentiles), and a *serving layer* (the API service merging both views). The decisions below are the ones that most shape the design.

1.  **Stateful detection backed by RocksDB.** The Kafka Streams topology (`MaritimeTopology`) keeps full per-MMSI state in a RocksDB-backed key/value store that survives restarts via its changelog topic. Three behavioral detectors run on this state:
    -   *Loitering* — low-speed dwell duration.
    -   *Speed anomaly* — Haversine-implied speed vs. reported SOG.
    -   *Dark vessel* — a wall-clock punctuator that scans for reporting gaps (silence). This is the correct pattern: the absence of messages cannot be detected from the stream itself, so a scheduled scan is required.

2.  **Detections emit to a separate topic.** Detection results are written to `maritime.detections` and never fed back into `maritime.enriched`. This structurally prevents processing loops and keeps the topology a clean DAG.

3.  **Serde / Schema Registry lifecycle management.** Avro Serdes (and their `SchemaRegistryClient`) are held as fields and closed in `@PreDestroy`, rather than created per topology build. This avoids leaking HTTP connection pools on every restart.

4.  **At-least-once semantics done correctly.** Kafka offsets are committed only *after* the downstream produce callback succeeds — never optimistically before the side effect — so no event is silently dropped on failure.

5.  **Port/adapter (hexagonal) design.** Storage and lookup concerns sit behind interfaces (`VesselStateStore`, `ColdTierWriter`, `PortDistanceProvider`) so backing implementations are swappable without touching callers. The Parquet cold tier uses Hive-style partitioning (`date=<yyyy-MM-dd>/mmsi=<mmsi>/...`) so Spark gets partition pruning for free.

6.  **Backward-compatible schema evolution.** Avro schemas use union-with-`null` fields and default detection flags to `false`, so new and old readers/writers remain compatible as the schema grows.

7.  **Observability and traceability built in.** Micrometer counters track each detection type, and correlation IDs propagate across async hops (Kafka headers + HTTP) via MDC, making a single event traceable end-to-end across services.

## Design Rationale — Plain Kafka vs. Kafka Streams

The `maritime-enricher` module deliberately uses **both** the plain consumer/producer API and the Kafka Streams API. The split is not stylistic — it follows a single rule:

> **Use plain Kafka when each message can be processed on its own. Reach for Kafka Streams the moment correctness depends on *other* messages — previous state, time windows, joins, or aggregations.**

| | Plain Kafka (`RiskScorerService`) | Kafka Streams (`MaritimeTopology`) |
|---|---|---|
| Nature | **Stateless** — one event in, one event out | **Stateful** — needs the vessel's history |
| API | `@KafkaListener` + `KafkaTemplate` | `StreamsBuilder` topology |
| State | None of its own | RocksDB `KeyValueStore`, one `VesselState` per MMSI |
| Example work | Validate, dedup, enrich, score | Loitering, speed-anomaly, dark-vessel detection |

**Why `RiskScorerService` stays plain.** Validation, zone enrichment, and scoring depend only on the event in hand. A plain consumer/producer is the simpler, lighter tool — no state stores, no topology, less to reason about. Kafka Streams here would add complexity that buys nothing.

**Why `MaritimeTopology` needs Streams.** Detecting that a vessel is *loitering* or *dark* requires comparing the current report against its past. Streams provides, out of the box, what you'd otherwise hand-build on top of a plain consumer:

1.  **Durable, partitioned state.** A per-MMSI store backed by a **changelog topic** — on restart it replays the changelog and resumes exactly where it left off. No external Redis/Postgres and no per-event network hop.
2.  **State co-partitioned with the data.** Streams guarantees a key's events and that key's state live on the same task, so a vessel's full history is always in one place. On a plain consumer, the same vessel's events could spread across instances and the detector would silently see only fragments.
3.  **Time and the punctuator.** The dark-vessel detector fires when events *stop* arriving — there is no message to react to. Streams' wall-clock `context.schedule(...)` punctuator scans the store on a timer; plain Kafka would need a separately managed scheduler coordinated with an external store.
4.  **Rebalance & exactly-once handling.** When an instance dies, Streams migrates the affected state (restored from the changelog) to the new owner, and can provide exactly-once consume→process→produce semantics. Doing this safely by hand is genuinely hard.

**The cost (why not Streams everywhere).** Kafka Streams pins you to the JVM, adds RocksDB on local disk plus internal changelog/repartition topics, and makes the app stateful — so deployment and scaling must respect partition assignment. For a stateless step like `RiskScorerService`, that overhead is pure cost with no benefit. Matching the tool to the statefulness of the work is the actual design decision.
