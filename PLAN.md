# Maritime Platform → Senior-Grade Portfolio MVP

## Context

`maritime-ingestion-system` is a Java/Spring Boot multi-module project that currently **compiles and runs the happy path** (ingestion → Kafka → streaming/enrichment → Kafka → storage → gateway) but reads as a *tutorial skeleton*, not senior work. It is a **portfolio MVP** meant to evidence fitness for a **Senior Backend (Java) Engineer** role on an AI-first maritime intelligence platform (Kafka streaming, AWS, scalable pipelines, data integrity, observability, ETL; nice-to-haves: Spark, agentic AI, geospatial/maritime domain).

The goal is **genuine learning** with the **full scope**, and to **understand the *why*** of each senior pattern. This plan evolves the skeleton into a credible senior work sample while teaching the rationale at every step.

**Confirmed decisions:**
- Schema technology: **Avro + Confluent Schema Registry** (`.avsc` → codegen, `KafkaAvroSerializer`, registry enforces compatibility at produce time).
- Execution cadence: **one phase at a time** — implement a phase fully, verify it compiles/runs, then **pause and explain what + why** before the next phase.

**Invariant:** the repo must compile after every phase.

---

## Current state (verified)

- Spring Boot 3.2.0, Java 17, builds on local JDK 25 via Lombok 1.18.42 + `annotationProcessorPaths` in parent `pom.xml`.
- 12 Java classes across `maritime-common`, `-ingestion` (8081), `-streaming` (8082), `-storage` (8083), `-gateway` (8084).
- DTOs are Lombok + `implements Serializable`, sent via Spring `JsonSerializer` with `spring.json.trusted.packages` (anti-pattern).
- `RiskScorerService`: one hardcoded rectangle zone, `random` distance-to-port; trivial scoring.
- `AwsStorageService`: S3 + DynamoDB via LocalStack, bucket/table created in `@PostConstruct`.
- **Absent:** any tests, Kafka `@Configuration` classes, schema registry, DLQ/retry/manual-ack/idempotent producer, validation/dedup, Micrometer/Prometheus (actuator only in ingestion+gateway), structured logging (uses `System.out`/`printStackTrace`), Spark, agentic artifacts, `.gitignore`/CI. `postgis` is in `docker-compose.yml` but **unused**.

### Build-fix history (already applied)
Before this plan, the repo did not compile. Fixed: `EnrichedVesselEvent.mmsi()` misuse, `final` field in a setter, invalid `spring-boot-starter-data-dynamodb`, Kafka `addListener`→`whenComplete`, unclosed JTS polygon ring, missing S3 bucket/DynamoDB table bootstrap, unused JPA/hibernate-spatial, lossy `int R = 6371e3`, Lombok JDK25 + annotation-processor path, AWS SDK `EndpointConfiguration` FQN + `putObject` metadata. `mvn clean install` now succeeds across all 6 modules.

---

## Phased plan

Order: **0 → 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8**. Each phase is independently shippable and ends with a verification + teaching pause.

### Phase 0 — Hygiene & build foundation
- **Proves:** baseline engineering credibility / automation.
- **Why (teach):** a reviewer judges the first 60 seconds — reproducible builds and CI signal seniority before any code is read.
- **Create:** `.gitignore`, Maven wrapper (`.mvn/wrapper/`, `mvnw`), `Makefile` (one-command stack up/build/run), `.github/workflows/ci.yml` (build + test). Remove committed `.DS_Store`.
- **Verify:** fresh `./mvnw clean verify` green; CI green.

### Phase 1 — Schema & contracts: Avro + Confluent Schema Registry  *(keystone)*
- **Proves:** data integrity at scale, contract discipline.
- **Why (teach):** schema-on-write (fail fast at produce, not at runtime deser); `BACKWARD` compatibility lets you add fields without coordinating the fleet; 4-byte schema-id framing = compact payloads; kills the `Serializable` + `trusted.packages` hole.
- **Create:** `maritime-common/src/main/avro/VesselEvent.avsc`, `EnrichedVesselEvent.avsc`; add `avro-maven-plugin` (codegen) + Confluent repo in parent `pom.xml`; replace hand-written DTOs with generated classes; swap all serializers to `KafkaAvroSerializer`/`KafkaAvroDeserializer`.
- **Infra:** add `schema-registry` to `docker-compose.yml` on **port 8085** (avoid clash with ingestion 8081).
- **Verify:** `curl :8085/subjects` lists subjects; negative test — an incompatible schema change is rejected with HTTP 409.

### Phase 2 — Kafka robustness: idempotent producer, manual acks, DLQ
- **Proves:** operating Kafka streaming solutions.
- **Why (teach):** decision is **at-least-once + idempotent producer + DLQ**, not full exactly-once (impossible across non-transactional S3/Dynamo sinks). `enable.idempotence`+`acks=all` dedupes producer retries; `AckMode.MANUAL_IMMEDIATE` commits only after the side-effect succeeds; `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` prevents one poison message blocking the partition.
- **Create:** `config/` packages per service (`KafkaProducerConfig`, `KafkaConsumerConfig`, `TopicConfig` with explicit `partitions=3`); listeners take `Acknowledgment`; `*.DLT` topics.
- **Verify:** malformed record lands in `.DLT` while the pipeline keeps flowing; kill streaming mid-batch → no loss after restart.

### Phase 3 — Data quality / ETL: Extract → Validate → Transform → Load
- **Proves:** ETL + data integrity / quality checks (a named JD responsibility).
- **Why (teach):** today is E→T→L with no V; real AIS has null-island `(0,0)`, spoofed MMSI, clock skew — quarantine, don't crash. Consumer-side dedup by `(mmsi, timestamp)` is the idempotency that at-least-once *requires*.
- **Create:** pure `VesselEventValidator` (lat/lon bounds, MMSI `^\d{9}$`, timestamp sanity, speed ≤ 102.2 kn), Caffeine-backed `DedupService`, `maritime.ais.quarantine` topic; validation runs **before** enrichment.
- **Deps:** Caffeine.
- **Verify:** bad record → quarantine topic with a reason header; replayed duplicate enriched only once.

### Phase 4 — Observability: Micrometer + Prometheus + Grafana + structured logs
- **Proves:** monitoring / bottleneck elimination.
- **Why (teach):** RED method (Rate, Errors, Duration); you can't claim to "optimize bottlenecks" without metrics. Correlation id propagated via Kafka headers + MDC makes one vessel greppable across all four services (no HTTP chain to carry trace context).
- **Create:** Micrometer + `micrometer-registry-prometheus` + actuator in **all** services (add to streaming + storage); custom metrics (`events_ingested/enriched/quarantined_total`, `risk_level_total{level}`, `dlq_total`, latency timers); SLF4J + JSON `logback-spring.xml`, replacing every `System.out`/`printStackTrace`; correlation-id filter/interceptor.
- **Infra:** add `prometheus` + `grafana` to `docker-compose.yml`.
- **Verify:** `/actuator/prometheus` counters increment; Grafana panels for risk-level mix + p99 latency; one MMSI's correlation id appears across three service logs.

### Phase 5 — Tests: unit + Testcontainers integration
- **Proves:** code quality, the single loudest gap (currently zero tests).
- **Why (teach):** test pyramid; Testcontainers runs **real** Kafka + Registry + LocalStack because mocks hide exactly the serialization/ack/DLQ bugs Phases 1–2 introduce. Refactor `RiskScorerService`'s `random` distance into an injected `PortDistanceProvider` — designing for testability is itself a senior signal.
- **Create:** `GeoUtilsTest`, validator/dedup/scorer unit tests; `PipelineIntegrationTest` (Kafka), `StorageIntegrationTest` (LocalStack); surefire/failsafe split in `pom.xml`.
- **Deps:** Testcontainers (kafka, localstack), Awaitility, AssertJ.
- **Verify:** `./mvnw verify` green; CI runs them.

### Phase 6 — Maritime analytics depth: stateful detection + real zones  *(domain centerpiece, largest scope)*
- **Proves:** domain credibility + scalable stateful streaming.
- **Decision:** **Kafka Streams** for temporal per-vessel state (RocksDB + changelog) + **PostGIS** as the authoritative geofence catalog (`ST_Contains`) — this finally *uses* the dead Postgres; a hardcoded `List<double[]>` doesn't scale.
- **Why (teach):** loitering (windowed low-speed), AIS-gap / "dark vessel" (punctuator timeout on missing reports), speed/heading anomaly (KTable of previous position + Haversine), zone types (EEZ / restricted / port) from GeoJSON with SRID 4326 + GiST index. This is the **speed layer** of a Lambda architecture (Phase 7 is the batch layer). Extending `EnrichedVesselEvent.avsc` here deliberately exercises Phase 1's BACKWARD compatibility.
- **Create:** `MaritimeTopology` + detectors; `ZoneRepository` (PostGIS, replaces `RESTRICTED_ZONE`); Flyway `V1__zones.sql` + GeoJSON resources; upgrade the simulator to realistic tracks (waypoints + one loiterer + one vessel that goes dark) so detectors actually fire.
- **Deps:** kafka-streams, postgresql, flyway. **Keep** `GeoUtils`/JTS (in-JVM math) — PostGIS complements, not replaces.
- **Verify:** stationary vessel → loitering event; silent vessel → dark-vessel event after timeout; point inside a GeoJSON polygon → `zoneType=RESTRICTED`; Streams state survives a restart.

### Phase 7 — Apache Spark: batch layer over the S3 cold tier
- **Proves:** large-scale processing (Spark nice-to-have) + Lambda architecture.
- **Decision:** standalone `maritime-spark` module (Spark Java API, `spark-submit` against LocalStack S3A → writes rollups to PostGIS). **Spark deps isolated** from the Spring services to avoid classpath conflicts.
- **Why (teach):** batch vs stream — exact 90-day rollups that streams can't cheaply compute; switch the S3 cold tier to **Parquet** partitioned by `date=/mmsi=` (revisits the Phase 1 storage-format decision). Jobs: daily per-vessel aggregates, risk rollups (p50/p95), loitering-hotspot grid mining.
- **Create:** `DailyVesselAggregatesJob`, `RiskRollupJob`, `SparkSessionFactory`; storage writes Parquet; gateway `GET /intelligence/{mmsi}/history`.
- **Verify:** `spark-submit` populates `vessel_daily_stats`; gateway history endpoint returns rollups.

---

## Verification (overall)

- After **every** phase: `./mvnw clean verify` compiles + (from Phase 5) passes tests.
- End-to-end smoke: `make up` (infra) → run 4 services → `POST :8081/api/v1/simulate/start` → observe metrics in Grafana, enriched/quarantine/DLT topics, S3/Dynamo writes, and `GET :8084/api/v1/intelligence/{mmsi}`.
- Phase-specific negative tests: 409 on incompatible schema; poison message → `.DLT`; bad AIS → quarantine; dark-vessel timeout fires; `spark-submit` rollups land in PostGIS.
