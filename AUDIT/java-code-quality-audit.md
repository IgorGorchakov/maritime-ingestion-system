# Java Code Quality Audit — maritime-ingestion-system

> **Update:** C1, C2, H1, M1, M2, L3, H4, L4, and H2 have been resolved and removed from this report.
> Surviving finding IDs are preserved (not renumbered), so earlier references remain valid.

## Summary

- **Scope reviewed:** All 5 Spring Boot services (`maritime-ingestion`, `maritime-enricher`, `maritime-detection`, `maritime-storage`, `maritime-api`) and `maritime-spark` read fully. `maritime-common` read fully. Generated Avro sources under `target/` skipped. Test classes (`src/test/java`) skipped per default scope.
- **Files reviewed:** 28 `.java` files across 7 modules
- **Findings:** 5 open (Critical: 0, High: 1, Medium: 2, Low: 2)
- **Top risks:** `FileSystemJsonColdTier` derives the cold-tier partition date from the JVM default timezone rather than UTC (H3), so at the midnight boundary events are silently written under the wrong `date=` partition and excluded from the correct day's Spark aggregates.

---

## Findings by severity

### 🟠 High

#### [H3] Cold-tier partition date derived from JVM default timezone — `maritime-storage/…/FileSystemJsonColdTier.java:41`

**Category:** Correctness / data integrity  
**What:**
```java
String isoDate = LocalDate.now().toString();   // uses JVM default timezone
```
`LocalDate.now()` returns the current date in the JVM's default timezone, which is typically the host's system timezone. The event's actual timestamp is an `Instant` (UTC), but the partition directory is named by wall-clock local date.  
**Why it matters:** At midnight in any timezone east of UTC, there is a window where `LocalDate.now()` returns the *next* day while the event's `Instant` timestamp still belongs to the *current* UTC day. Spark jobs that read `date=<yyyy-MM-dd>` partitions will see events under a partition date that does not match their UTC timestamp. The `DailyVesselAggregatesJob` filters `col("date").equalTo(props.getBatchDate())` — events misassigned to the wrong partition will be silently excluded from the batch run for their real date and included in the wrong day's aggregates. In a CET (UTC+1) timezone this window is one hour wide every night.

**Suggested fix:**
```java
// Before
String isoDate = LocalDate.now().toString();

// After
String isoDate = LocalDate.now(ZoneOffset.UTC).toString();
```

---

### 🟡 Medium

#### [M3] Risk-scoring constants duplicated across two classes

**Category:** Coupling / cohesion — DRY  
**What:** The risk-scoring weights and thresholds are defined as inline literals in both `RiskScorerEnrichService` and `HexCrossingEnricherService`, with no shared constant:

In `RiskScorerEnrichService` (~line 154):
```java
if (inRestrictedZone)             riskScore += 50;
else if ("PORT".equals(zoneType)) riskScore += 20;
else if ("EEZ".equals(zoneType))  riskScore += 10;
if (distanceToPort < 10)          riskScore += 20;
if (event.getSpeed() > 25)        riskScore += 10;
String riskLevel = riskScore >= 50 ? "HIGH" : riskScore >= 20 ? "MEDIUM" : "LOW";
```

In `HexCrossingEnricherService` (~line 65): identical numeric and string literals for the same model.  
**Why it matters:** The risk model appears in two separate service classes. Adjusting a threshold (e.g., raising the near-port proximity from 10 NM to 15 NM) requires finding and changing both files. Given that both services produce events consumed by the same downstream Spark jobs, inconsistency between the two risk models would silently produce contradictory risk labels in the same pipeline.

**Suggested fix:**
```java
// In maritime-common (or a shared RiskScoringPolicy class in maritime-enricher)
public final class RiskPolicy {
    public static final double RESTRICTED_ZONE_WEIGHT = 50.0;
    public static final double PORT_ZONE_WEIGHT        = 20.0;
    public static final double EEZ_ZONE_WEIGHT         = 10.0;
    public static final double NEAR_PORT_WEIGHT        = 20.0;
    public static final double HIGH_SPEED_WEIGHT       = 10.0;
    public static final double NEAR_PORT_THRESHOLD_NM  = 10.0;
    public static final double HIGH_SPEED_THRESHOLD_KN = 25.0;
    public static final double HIGH_RISK_THRESHOLD     = 50.0;
    public static final double MEDIUM_RISK_THRESHOLD   = 20.0;

    public static String toRiskLevel(double score) {
        return score >= HIGH_RISK_THRESHOLD ? "HIGH"
             : score >= MEDIUM_RISK_THRESHOLD ? "MEDIUM"
             : "LOW";
    }
}
```

---

#### [M4] `new Random()` instantiated per call in a hot path — `maritime-enricher/…/RandomPortDistanceProvider.java:42`

**Category:** Resource inefficiency  
**What:**
```java
double distanceNm = new Random().nextDouble() * 100;
```
`RandomPortDistanceProvider` is a singleton Spring bean. `new Random()` seeds itself from `System.nanoTime()`, which involves a system call on each of the (currently) 4–5 events per second. The Javadoc in the class itself acknowledges this.  
**Why it matters:** While the absolute cost is low for a demo fleet, the pattern is wrong: a singleton bean that allocates a new `Random` on every hot-path invocation is a code pattern that gets copied. `ThreadLocalRandom` is the standard solution — it is thread-safe without synchronization and avoids allocation:

**Suggested fix:**
```java
// Before
double distanceNm = new Random().nextDouble() * 100;

// After
double distanceNm = ThreadLocalRandom.current().nextDouble(100.0);
```

---

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
| `FileSystemJsonColdTier` | `maritime-storage` | H3 | High — data-correctness at timezone boundary |
| `RiskScorerEnrichService` / `HexCrossingEnricherService` | `maritime-enricher` | M3 | Medium — duplicated risk model |
| `RandomPortDistanceProvider` | `maritime-enricher` | M4 | Medium — `new Random()` per call in hot path |

**Remaining open findings:** H3, M3, M4, L1, L2.

---

## What was not covered

- **`src/test/java`** — test classes were excluded from this audit per default skill scope.
- **Generated Avro sources** under `target/generated-sources/avro/` — skipped (annotated `@Generated` / not authored code).
- **Frontend (`maritime-frontend`)** — JavaScript/React out of scope for a Java audit.
- **SQL migrations** (`src/main/resources/db/migration/`) — not Java; not audited.
- **Configuration files** (`application.properties`, `docker-compose.yml`) — not Java; not audited.
- **`maritime-common` interceptors and filters** (`CorrelationIdProducerInterceptor`, `CorrelationIdRecordInterceptor`, `CorrelationIdHttpFilter`) — read but no findings; clean propagation pattern, no resource leaks.
- **Kafka producer/consumer configs** (`KafkaProducerConfig`, `KafkaConsumerConfig`, `KafkaConsumerConfig` in storage) — read and reviewed; no findings beyond what is already covered in H2.
