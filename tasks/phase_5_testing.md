# Phase 5: Tests — Unit + Testcontainers Integration

> Part of the senior-MVP roadmap in `PLAN.md`. Sequence: 0 → … → 4 → **5** → 6 → 7 → 8.
> Status: ☐ Not started

## Objective
Introduce a real test suite — the single loudest current gap (zero tests exist).

## JD competency proven
Code quality; reliability.

## Why (rationale to internalize)
- **Test pyramid:** many fast unit tests, fewer integration tests.
- **Testcontainers** spins up REAL Kafka + Schema Registry + LocalStack, because mocks hide exactly the serialization / ack / DLQ behavior introduced in Phases 1–2.
- Refactoring `RiskScorerService`'s `random` distance into an injected `PortDistanceProvider` makes scoring deterministic and testable — designing for testability is itself a senior signal.

## Steps
1. Unit tests: `GeoUtilsTest` (point-in-polygon, Haversine), `VesselEventValidatorTest`, `DedupServiceTest`, `RiskScorerTest` (with injected `PortDistanceProvider`).
2. Integration: `PipelineIntegrationTest` (Testcontainers Kafka + Registry) asserting raw → enriched; `StorageIntegrationTest` (Testcontainers LocalStack) asserting S3 + Dynamo writes.
3. Split surefire (unit) vs failsafe (`*IT`, integration) in `pom.xml`.
4. Ensure CI (Phase 0) runs `./mvnw verify` including integration tests.

## Deliverables
- Unit + integration tests; surefire/failsafe split; green CI.

## Dependencies
- Testcontainers (kafka, localstack), Awaitility, AssertJ.

## Verification
- `./mvnw verify` is green locally and in CI.
- Integration tests fail correctly if serialization/ack wiring regresses.
