# Phase 1: Schema & Contracts — Avro + Confluent Schema Registry (KEYSTONE)

> Part of the senior-MVP roadmap in `PLAN.md`. Sequence: 0 → **1** → … → 8.
> Status: ☐ Not started

## Objective
Replace Java-serialized DTOs sent via Spring `JsonSerializer` with Avro records governed by a Schema Registry.

## JD competency proven
Data integrity at scale; contract discipline.

## Why (rationale to internalize)
- **Schema-on-write:** the registry rejects incompatible payloads at *produce* time (fail fast), not at runtime deserialization on some downstream consumer.
- **BACKWARD compatibility:** add fields without coordinating every consumer deploy.
- **Compact framing:** 4-byte schema id + binary body instead of fat JSON with embedded type headers.
- Eliminates the `implements Serializable` + `spring.json.trusted.packages` anti-pattern (a deserialization-gadget risk).

## Steps
1. Create `maritime-common/src/main/avro/VesselEvent.avsc` and `EnrichedVesselEvent.avsc`.
2. Add `avro-maven-plugin` (codegen) and the Confluent Maven repo to parent `pom.xml`; add `kafka-avro-serializer` dep.
3. Delete hand-written `VesselEvent`/`EnrichedVesselEvent` DTOs; switch all code to generated classes.
4. Swap serializers/deserializers to `KafkaAvroSerializer` / `KafkaAvroDeserializer`; configure `schema.registry.url` + `specific.avro.reader=true`.
5. Add `schema-registry` service to `docker-compose.yml` on **port 8085** (NOT 8081 — clashes with ingestion).

## Deliverables
- `.avsc` schemas + generated Avro classes used end-to-end.
- Schema Registry running; producers/consumers wired to it.

## Verification
- `curl http://localhost:8085/subjects` lists registered subjects.
- Negative test: registering an incompatible schema returns HTTP 409.
- Full pipeline still flows ingestion → storage with Avro on the wire.

## Risks / constraints
- Port 8085 for registry (avoid 8081 collision).
- Opens the S3 cold-tier format question (JSON vs Parquet) — finalized in Phase 7.
