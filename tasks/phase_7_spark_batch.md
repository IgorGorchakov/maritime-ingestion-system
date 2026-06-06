# Phase 7: Apache Spark — Batch Layer over the S3 Cold Tier

> Part of the senior-MVP roadmap in `PLAN.md`. Sequence: 0 → … → 6 → **7** → 8.
> Status: ☐ Not started

## Objective
Add a batch analytics layer that computes historical rollups over the S3 cold tier — the batch half of a Lambda architecture.

## JD competency proven
Large-scale data processing (Apache Spark, nice-to-have) + Lambda architecture literacy.

## Why (rationale to internalize)
- **Batch vs stream:** exact 90-day rollups and full-history pattern mining are expensive/awkward in a streaming topology but natural in batch.
- Switch the S3 cold tier to **Parquet** partitioned by `date=/mmsi=` (columnar + partition pruning) — this finalizes the storage-format question opened in Phase 1.
- **Dependency isolation:** Spark pulls a heavy, conflict-prone dependency tree; keep it in a standalone module away from the Spring services.

## Steps
1. New standalone module `maritime-spark` (Spark Java API), isolated deps; `SparkSessionFactory` configured for S3A → LocalStack.
2. Storage (Phase 1/this phase) writes cold tier as Parquet partitioned by `date/mmsi`.
3. Jobs: `DailyVesselAggregatesJob` (per-vessel daily stats), `RiskRollupJob` (p50/p95 risk), loitering-hotspot grid mining; write results to PostGIS.
4. Gateway: `GET /api/v1/intelligence/{mmsi}/history` returns the rollups.

## Deliverables
- `maritime-spark` module + jobs; Parquet cold tier; history endpoint.

## Dependencies
- spark-sql, hadoop-aws / S3A (isolated to the Spark module).

## Verification
- `spark-submit` populates `vessel_daily_stats` in PostGIS.
- Gateway history endpoint returns the computed rollups.

## Risks / constraints
- Spark dependency isolation is mandatory (standalone/shaded) to avoid clashing with Spring Boot.
