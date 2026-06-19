Let me read everything relevant before making any recommendations.Good. I now have the complete picture. Here's my investigation.

---

## Investigation: extracting `maritime-detection` as a new module

### What needs to move

The detection subsystem currently living in `maritime-enricher` consists of exactly these files:

| File | Role |
|---|---|
| `streams/MaritimeTopology.java` | Topology wiring, lifecycle (`@PostConstruct` / `@PreDestroy`) |
| `streams/VesselDetectionProcessor.java` | All three detector algorithms |
| `streams/VesselState.java` | Per-vessel state POJO |
| `streams/VesselStateSerdes.java` | JSON serde for RocksDB + changelog |
| `streams/AvroEnrichedEventSerdes.java` | Avro serde wrapping Schema Registry |
| `config/TopicConfig.java` — **partially** | Only the `maritimeDetections()` and `maritimeStateChangelog()` beans belong to detection |

Everything else in `maritime-enricher` stays — `RiskScorerService`, `DedupService`, `PortDistanceProvider`, `ZoneRepository`, `KafkaConsumerConfig`, `KafkaProducerConfig`, `PipelineConfig`, Flyway migrations, and `PipelineIntegrationIT`.

---

### The new module's dependency graph

```
maritime-common          (Avro DTOs, Topics, GeoUtils)
        ↑
maritime-detection       (new — Kafka Streams topology + detectors)
```

`maritime-detection` does **not** depend on `maritime-enricher`. It reads from `maritime.enriched` (a topic name constant in `maritime-common`) and writes to `maritime.detections`. The enricher and the detection service are fully decoupled at the data level — they communicate only through Kafka.

---

### What `maritime-detection` needs from `maritime-enricher`

Nothing. The only shared surface is `Topics.*` constants (already in `maritime-common`) and `EnrichedVesselEvent` / `VesselEvent` (Avro types also in `maritime-common`). `GeoUtils.calculateDistanceInMeters` used by the speed anomaly detector is also already in `maritime-common`. Zero new shared code is needed.

---

### What stays in `maritime-enricher` and why

`KafkaConsumerConfig` — the listener container factory, DLQ recoverer, and ack mode are all consumer-side concerns for `RiskScorerService`. The detection topology uses the Kafka Streams native client, not Spring Kafka's `@KafkaListener`, so it needs none of this.

`TopicConfig` — the `maritimeDetections()` and `maritimeStateChangelog()` beans are the only ones that belong to detection. However, topic declarations are idempotent in Kafka (`KafkaAdmin` uses `CreateTopics` which is a no-op if the topic already exists). The cleanest split: move `maritimeDetections()` and `maritimeStateChangelog()` to `maritime-detection`, leave the rest in `maritime-enricher`.

`Flyway migrations` — `V1__zones.sql` (PostGIS zones catalog) and `V2__spark_batch_tables.sql` stay in `maritime-enricher` since they're required by `ZoneRepository`, which `RiskScorerService` depends on. The detection service doesn't touch Postgres at all — it's pure Kafka Streams / RocksDB.

`spring-boot-starter-data-jpa` + `postgresql` — stay in `maritime-enricher`. Detection needs neither.

`PipelineIntegrationIT` — tests the enricher ETL pipeline only (raw → enriched, quarantine). The topology has its own test in `maritime-detection` (a new `DetectionTopologyIT`).

---

### Dependency changes

**`maritime-detection/pom.xml`** needs:
- `maritime-common` (Avro DTOs, Topics constants, GeoUtils)
- `spring-boot-starter-web` (thin — only for actuator endpoints; could be `spring-boot-starter` if you don't want a web layer)
- `spring-boot-starter-actuator` + `micrometer-registry-prometheus` (observability)
- `kafka-streams` (the Streams DSL)
- `kafka-avro-serializer` (for `AvroEnrichedEventSerdes` → Schema Registry)
- `jackson-datatype-jsr310` (for `VesselStateSerdes` → `Instant`)
- Test: `testcontainers:kafka`, `junit-jupiter`, `awaitility`, `spring-kafka-test`

**`maritime-enricher/pom.xml`** drops:
- `kafka-streams` — no longer used after the topology moves out
- The `kafka-avro-serializer` stays (still needed for `KafkaAvroSerializer` in `KafkaConsumerConfig`)

---

### One non-obvious issue: `TopicConfig` split

`TopicConfig` in `maritime-enricher` currently declares all six topics including `maritimeDetections` and `maritimeStateChangelog`. After the split, if `maritime-enricher` starts first (which it will on `make run-enricher` since that's port 8082), the detections topic doesn't exist yet and any consumer subscribed to it will get a `UNKNOWN_TOPIC_OR_PARTITION` error.

The two safe options are:

1. **Keep all topic declarations in `maritime-enricher`** and accept that the enricher owns topic provisioning for the whole platform. Simple, and since the enricher starts first in practice, the topics exist before the detection service needs them.
2. **Move detection topic declarations into `maritime-detection`** and rely on Kafka's auto-create behaviour or a separate admin job. Cleaner in principle, fragile in practice without `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true` in docker-compose.

**Recommendation: option 1** for now. Add a comment in `TopicConfig` noting that it acts as the platform-wide topic registry (same role `maritime-common/Topics.java` plays for names). This is the pragmatic choice for a local portfolio project.

---

### Port assignment

`maritime-enricher` is on `8082`. `maritime-detection` needs its own port. **`8086`** is free (8085 is Schema Registry, 8083 is storage, 8084 is API).

---

### Application class package

The new module's main class goes in package `com.maritime.detection` with a `DetectionApplication`. The streams classes move from `com.maritime.enricher.streams` → `com.maritime.detection.streams`; the topic config moves to `com.maritime.detection.config`.

---

### Impact on `docker-compose.yml`, `Makefile`, `README.md`, Prometheus config

`docker-compose.yml` — no change (the service runs on the host via `make run-*`, not as a container).

`Makefile` — add `run-detection` target: `$(MVNW) -pl maritime-detection spring-boot:run`.

`infra/prometheus/prometheus.yml` — add a scrape target for `host.docker.internal:8086`.

`README.md` — update the module table (add `maritime-detection` row), architecture diagram, topics table.

`parent pom.xml` — add `<module>maritime-detection</module>`.

---

### Migration checklist (in execution order)

1. Create `maritime-detection/` directory structure
2. Write `maritime-detection/pom.xml`
3. Create `DetectionApplication.java` in `com.maritime.detection`
4. Move + repackage the five streams classes (`MaritimeTopology`, `VesselDetectionProcessor`, `VesselState`, `VesselStateSerdes`, `AvroEnrichedEventSerdes`) to `com.maritime.detection.streams`
5. Create `maritime-detection/src/main/resources/application.properties` (port 8086, streams config)
6. Create `DetectionTopologyConfig` in `com.maritime.detection.config` with `maritimeDetections()` and `maritimeStateChangelog()` beans
7. Remove the five streams classes from `maritime-enricher`
8. Remove `kafka-streams` from `maritime-enricher/pom.xml`
9. Remove the two detection-specific beans from `maritime-enricher/config/TopicConfig.java` (or leave as option 1 above)
10. Add `<module>maritime-detection</module>` to root `pom.xml`
11. Write `DetectionTopologyIT` in `maritime-detection`
12. Update `Makefile`, Prometheus config, `README.md`

---

Ready to implement all of this? I can execute it all in one pass.