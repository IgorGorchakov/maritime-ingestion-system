# Phase 3: Data Quality / ETL — Extract → Validate → Transform → Load

> Part of the senior-MVP roadmap in `PLAN.md`. Sequence: 0 → 1 → 2 → **3** → … → 8.
> Status: ◐ Code-complete, uncommitted — Validator + DedupService + quarantine topic + E→V→T→L ordering in place; compiles green. Fixed during review: DedupService.isDuplicate always returned false (now putIfAbsent); Validator + DedupService were never registered as beans (now PipelineConfig). ⚠ Open: quarantine path acks before the async send completes (events lost if quarantine produce fails). ⚠ Runtime checks (bad record→quarantine w/ reason header; replay→enriched once) not yet executed.

## Objective
Insert an explicit validation + dedup stage before enrichment, quarantining bad data instead of crashing.

## JD competency proven
ETL processes + data integrity / quality checks (a named JD responsibility).

## Why (rationale to internalize)
- The pipeline today is E→T→L with **no V**. Real AIS contains null-island `(0,0)`, spoofed/short MMSIs, and clock skew.
- **Quarantine, don't crash:** bad records routed to a quarantine topic with a reason, pipeline stays up.
- **Dedup by `(mmsi, timestamp)`** is the consumer-side idempotency that at-least-once delivery (Phase 2) *requires* to avoid double-processing.

## Steps
1. `VesselEventValidator` (pure, unit-testable): lat ∈ [-90,90], lon ∈ [-180,180], not (0,0), MMSI `^\d{9}$`, timestamp within sane window, speed ≤ 102.2 kn.
2. `DedupService` backed by Caffeine (TTL cache of seen `(mmsi,timestamp)` keys).
3. New topic `maritime.ais.quarantine`; invalid records published there with a `reason` header.
4. Wire validation + dedup to run **before** enrichment in the streaming consumer.

## Deliverables
- Validator + dedup components; quarantine topic; E→V→T→L ordering.

## Dependencies
- Caffeine.

## Verification
- A record with lat=999 / MMSI="abc" → quarantine topic with reason header; not enriched.
- The same `(mmsi,timestamp)` replayed twice → enriched exactly once.
