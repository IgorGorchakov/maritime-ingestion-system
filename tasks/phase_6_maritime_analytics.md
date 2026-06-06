# Phase 6: Maritime Analytics Depth — Stateful Detection + Real Zones (DOMAIN CENTERPIECE)

> Part of the senior-MVP roadmap in `PLAN.md`. Sequence: 0 → … → 5 → **6** → 7 → 8.
> Status: ☐ Not started — largest scope.

## Objective
Replace trivial scoring (one rectangle, random distance) with real stateful maritime behavioral analytics and a proper geofence catalog.

## JD competency proven
Maritime/geospatial domain credibility + scalable stateful streaming. This is the **speed layer** of a Lambda architecture (Phase 7 = batch layer).

## Why (rationale to internalize)
- **Kafka Streams** holds per-vessel temporal state (RocksDB + changelog topic) so detection survives restarts and scales by partition.
- **PostGIS** becomes the authoritative geofence catalog (`ST_Contains`, SRID 4326, GiST index) — this finally *uses* the dead Postgres container; a hardcoded `List<double[]>` doesn't scale.
- Detectors model real behaviors analysts care about.
- Extending `EnrichedVesselEvent.avsc` here deliberately exercises Phase 1's BACKWARD compatibility.

## Steps
1. `MaritimeTopology` (Kafka Streams) with detectors:
   - **Loitering:** windowed low-speed dwell.
   - **Dark vessel / AIS gap:** punctuator fires when expected reports stop for N minutes.
   - **Speed/heading anomaly:** KTable of previous position; Haversine-derived implied speed vs reported.
2. `ZoneRepository` (PostGIS) replacing `RESTRICTED_ZONE`; zone types EEZ / restricted / port from GeoJSON.
3. Flyway `V1__zones.sql` (PostGIS extension, GiST index) + GeoJSON seed resources.
4. Upgrade the simulator to realistic tracks: waypoint movement + one deliberate loiterer + one vessel that goes dark, so detectors actually fire.
5. Extend `EnrichedVesselEvent.avsc` with new detection fields (BACKWARD-compatible).

## Deliverables
- Kafka Streams topology with 3 detectors; PostGIS zone catalog; richer simulator.

## Dependencies
- kafka-streams, postgresql (JDBC), flyway. Keep `GeoUtils`/JTS (in-JVM math complements PostGIS).

## Verification
- Stationary vessel → loitering event.
- Silent vessel → dark-vessel event after the timeout.
- Point inside a seeded GeoJSON polygon → `zoneType=RESTRICTED`.
- Streams state survives a service restart.
