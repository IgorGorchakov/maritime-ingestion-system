# Java Code Quality Audit — maritime-ingestion-system

> **Update:** C1, C2, H1, M1, M2, L3, H4, L4, H2, and H3 have been resolved and removed from this report.
> Surviving finding IDs are preserved (not renumbered), so earlier references remain valid.

## Summary

- **Scope reviewed:** All 5 Spring Boot services (`maritime-ingestion`, `maritime-enricher`, `maritime-detection`, `maritime-storage`, `maritime-api`) and `maritime-spark` read fully. `maritime-common` read fully. Generated Avro sources under `target/` skipped. Test classes (`src/test/java`) skipped per default scope.
- **Files reviewed:** 28 `.java` files across 7 modules
- **Findings:** 2 open (Critical: 0, High: 0, Medium: 0, Low: 2)
- **Top risks:** Utility class `GeoUtils` missing private constructor (L1). Magic string `"AIS"` hardcoded in two classes with no shared constant (L2).

---

## Findings by severity

### 🔵 Low

#### [L1] Utility class missing private constructor — `maritime-common/…/GeoUtils.java`

**Category:** Naming / design  
**What:** `GeoUtils` is a pure static utility class — it holds two `private static final` singletons (`GEOMETRY_FACTORY`, `H3`) and exposes only static methods. There is no private no-arg constructor to prevent instantiation.  
**Why it matters:** Calling `new GeoUtils()` compiles and runs. While harmless here (the static initializer runs the same), a missing private constructor signals to both readers and tools (e.g., PMD) that the class may be intended for subclassing or instantiation.

**Suggested fix:**
```java
public class GeoUtils {
    private GeoUtils() {}   // prevent instantiation
    // ...
}
```

---

#### [L2] Magic string `"AIS"` used for event type in two places

**Category:** Magic strings  
**What:** The literal `"AIS"` is hardcoded twice as the `eventType` field value:
- `AisSimulatorService.vesselEvent()`: `.setEventType("AIS")`
- `VesselDetectionProcessor.toEnrichedEvent()`: `.setEventType("AIS")`

**Why it matters:** If the event type taxonomy gains a second value (e.g., `"SYNTHETIC"` for test data, `"REPLAY"` for historical playback), there is no compile-time guidance on where `"AIS"` is used. A typo in either location produces a silently wrong value in the Kafka stream.

**Suggested fix:**
```java
// In Topics.java or a new EventTypes.java in maritime-common
public final class EventTypes {
    public static final String AIS = "AIS";
    private EventTypes() {}
}
```

---

## Hotspots

| Class | Module | Findings | Priority |
|:---|:---|:---|:---|
| `GeoUtils` | `maritime-common` | L1 | Low — missing private constructor on utility class |
| `AisSimulatorService` / `VesselDetectionProcessor` | `maritime-ingestion` / `maritime-detection` | L2 | Low — magic string `"AIS"` in two classes |

**Remaining open findings:** L1, L2.

---

## What was not covered

- **`src/test/java`** — test classes were excluded from this audit per default skill scope.
- **Generated Avro sources** under `target/generated-sources/avro/` — skipped (annotated `@Generated` / not authored code).
- **Frontend (`maritime-frontend`)** — JavaScript/React out of scope for a Java audit.
- **SQL migrations** (`src/main/resources/db/migration/`) — not Java; not audited.
- **Configuration files** (`application.properties`, `docker-compose.yml`) — not Java; not audited.
- **`maritime-common` interceptors and filters** (`CorrelationIdProducerInterceptor`, `CorrelationIdRecordInterceptor`, `CorrelationIdHttpFilter`) — read but no findings; clean propagation pattern, no resource leaks.
- **Kafka producer/consumer configs** (`KafkaProducerConfig`, `KafkaConsumerConfig`, `KafkaConsumerConfig` in storage) — read and reviewed; no findings beyond what was covered in H2 (resolved).
