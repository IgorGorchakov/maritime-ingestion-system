package com.maritime.spark;

import com.maritime.spark.jobs.DailyVesselAggregatesJob;
import com.maritime.spark.jobs.LoiteringHotspotJob;
import com.maritime.spark.jobs.RiskRollupJob;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.*;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for the three Spark batch jobs: {@link DailyVesselAggregatesJob},
 * {@link RiskRollupJob}, and {@link LoiteringHotspotJob}.
 *
 * <h3>Strategy — no Postgres, no Testcontainers</h3>
 * All three jobs share the same structure:
 * <ol>
 *   <li>Read Parquet from a cold-tier path.</li>
 *   <li>Transform in-memory (filter → groupBy → agg).</li>
 *   <li>Write to Postgres via JDBC ({@code writeToPostgres}).</li>
 * </ol>
 * Steps 1 and 2 are tested here by writing synthetic Parquet to a
 * {@code @TempDir} and routing JDBC output to an H2 in-memory database.
 * Step 3 is already covered end-to-end by {@code StorageIntegrationIT}.
 *
 * <h3>Why H2 and not a mock?</h3>
 * {@code writeToPostgres} is package-private and static — not interceptable by
 * Mockito without PowerMock. H2 with {@code MODE=PostgreSQL} accepts Spark's
 * {@code df.write().mode(Overwrite).jdbc()} for the column types used here
 * (BIGINT, DOUBLE, VARCHAR, DATE) and auto-creates the table from Spark's
 * inferred schema. This gives us a real end-to-end execution path with no
 * external infrastructure.
 *
 * <h3>SparkSession lifecycle</h3>
 * One {@code local[1]} session is shared across all tests via
 * {@code @BeforeAll} / {@code @AfterAll}. SparkSession startup costs ~3–5 s;
 * sharing keeps the full suite under ~30 s. Each test writes to its own
 * named H2 database ({@code jdbc:h2:mem:<testName>}) to prevent table collisions.
 *
 * <h3>callUDF audit — RiskRollupJob</h3>
 * {@link RiskRollupJob} uses {@code expr("approx_percentile(...)")} — NOT
 * {@code callUDF("approx_percentile", ...)}. If {@code callUDF} were used for a
 * Spark built-in, an {@code AnalysisException: Undefined function} would be
 * thrown at plan-analysis time (before any data is read). The test at
 * {@link #riskRollup_percentiles_computedWithoutCallUDF} serves as a regression
 * guard: if someone accidentally changes {@code expr()} to {@code callUDF()},
 * the test fails with that exception before any assertion runs.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SparkJobsTest {

    // ── Shared SparkSession ───────────────────────────────────────────────────

    static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .appName("SparkJobsTest")
                .master("local[1]")
                .config("spark.ui.enabled",                "false")
                .config("spark.sql.adaptive.enabled",      "false")
                // Keep shuffle partitions at 1 — tests run on tiny datasets.
                // The default 200 causes Spark to create 200 empty task files.
                .config("spark.sql.shuffle.partitions",    "1")
                .getOrCreate();

        // Silence Spark's verbose INFO logging so test output is readable.
        // Keep WARN so actual job warnings remain visible.
        org.apache.log4j.Logger.getLogger("org.apache.spark")
                .setLevel(org.apache.log4j.Level.WARN);
        org.apache.log4j.Logger.getLogger("org.apache.hadoop")
                .setLevel(org.apache.log4j.Level.WARN);
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) spark.stop();
    }

    // ── Shared schema ─────────────────────────────────────────────────────────

    static final String BATCH_DATE = "2024-03-15";

    /**
     * Parquet schema used to write fixture data.
     *
     * Mirrors the Avro schema in {@code EnrichedVesselEvent.avsc} as a Spark
     * {@link StructType}. The nested {@code vesselEvent} struct corresponds to
     * {@code VesselEvent.avsc}. The trailing {@code date} and {@code mmsi_key}
     * fields are the Hive partition columns; {@code mmsi_key} is dropped before
     * the write so only {@code date} becomes a partition directory.
     */
    static final StructType VESSEL_EVENT_STRUCT = new StructType()
            .add("mmsi",      DataTypes.StringType, false)
            .add("latitude",  DataTypes.DoubleType, false)
            .add("longitude", DataTypes.DoubleType, false)
            .add("speed",     DataTypes.DoubleType, false)
            .add("heading",   DataTypes.DoubleType, false)
            .add("timestamp", DataTypes.LongType,   false)
            .add("eventType", DataTypes.StringType, false);

    static final StructType ENRICHED_SCHEMA = new StructType()
            .add("vesselEvent",      VESSEL_EVENT_STRUCT,     false)
            .add("inRestrictedZone", DataTypes.BooleanType,   false)
            .add("zoneName",         DataTypes.StringType,    true)
            .add("zoneType",         DataTypes.StringType,    true)
            .add("distanceToPort",   DataTypes.DoubleType,    false)
            .add("riskScore",        DataTypes.DoubleType,    false)
            .add("riskLevel",        DataTypes.StringType,    false)
            .add("loitering",        DataTypes.BooleanType,   false)
            .add("darkVessel",       DataTypes.BooleanType,   false)
            .add("speedAnomaly",     DataTypes.BooleanType,   false)
            .add("date",             DataTypes.StringType,    false)  // Hive partition col
            .add("mmsi_key",         DataTypes.StringType,    false); // dropped before write

    // ── Fixture helpers ───────────────────────────────────────────────────────

    /**
     * Write {@code rows} as Parquet under
     * {@code file://<tempDir>/vessel-events/date=<BATCH_DATE>/}.
     *
     * We use Spark's generic {@link RowFactory} rather than the Avro
     * {@code SpecificRecord} builders because the Avro-generated classes are
     * compiled by {@code avro-maven-plugin} and are not available during unit test
     * compilation in this module. Using generic rows keeps the test self-contained.
     *
     * @return the {@code file://} base path Spark should read from
     */
    static String writeFixtureParquet(Path tempDir, List<Row> rows) {
        Dataset<Row> df = spark.createDataFrame(rows, ENRICHED_SCHEMA);
        String basePath = "file://" + tempDir.toAbsolutePath() + "/vessel-events";

        df.drop("mmsi_key")   // drop the helper column; only date= partitions the path
          .write()
          .mode("overwrite")
          .partitionBy("date")
          .parquet(basePath);

        return basePath;
    }

    /**
     * Build a fixture Row with position fixed at (lat=30.5, lon=-89.0).
     * Caller controls the fields that vary across test scenarios.
     */
    static Row row(String mmsi, double speed, double riskScore,
                   boolean loitering, boolean inRestrictedZone) {
        return row(mmsi, 30.5, -89.0, speed, riskScore, loitering, inRestrictedZone);
    }

    /**
     * Build a fixture Row with explicit lat/lon — used by loitering grid tests.
     */
    static Row row(String mmsi, double lat, double lon, double speed,
                   double riskScore, boolean loitering, boolean inRestrictedZone) {
        // Inner struct — field order must match VESSEL_EVENT_STRUCT exactly.
        Row vesselEvent = RowFactory.create(
                mmsi, lat, lon, speed, 90.0, System.currentTimeMillis(), "AIS");

        // Outer Row — field order must match ENRICHED_SCHEMA exactly.
        return RowFactory.create(
                vesselEvent,
                inRestrictedZone,
                null,          // zoneName
                null,          // zoneType
                25.0,          // distanceToPort
                riskScore,
                riskScore >= 50 ? "HIGH" : riskScore >= 20 ? "MEDIUM" : "LOW",
                loitering,
                false,         // darkVessel
                false,         // speedAnomaly
                BATCH_DATE,    // date — becomes the Hive partition directory
                mmsi           // mmsi_key — dropped before write
        );
    }

    /** Unique H2 in-memory JDBC URL per test — prevents table name collisions. */
    static String h2Url(String dbName) {
        // DB_CLOSE_DELAY=-1: keep the database alive for the duration of the JVM
        // so the assertion JDBC connection can read what Spark wrote.
        // MODE=PostgreSQL: accept Spark's SaveMode.Overwrite truncate behaviour.
        return "jdbc:h2:mem:" + dbName + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
    }

    /**
     * Construct a {@link JobConfig} for testing without touching env variables.
     *
     * {@code JobConfig} exposes only a static factory ({@link JobConfig#fromEnv()}).
     * We reach the private constructor via reflection to inject test values. This
     * is preferable to adding a test-only constructor to the production class,
     * which would leak test concerns into the main API.
     */
    static JobConfig cfg(Path tempDir, String h2Url) {
        try {
            var ctor = JobConfig.class.getDeclaredConstructor(
                    String.class, String.class, String.class, String.class, String.class);
            ctor.setAccessible(true);
            return ctor.newInstance(
                    tempDir.toAbsolutePath().toString(), // coldTierDir
                    h2Url,                               // dbUrl  → H2
                    "sa",                                // dbUser (H2 default)
                    "",                                  // dbPass (H2 default)
                    BATCH_DATE                           // batchDate
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to construct test JobConfig via reflection", e);
        }
    }

    // ── DailyVesselAggregatesJob ──────────────────────────────────────────────

    @Test
    @Order(1)
    @DisplayName("DailyVesselAggregatesJob — one aggregated row per MMSI with correct values")
    void dailyAggregates_oneRowPerVessel_correctAggregates(@TempDir Path tempDir) throws Exception {
        // Vessel A: 3 events. Vessel B: 2 events.
        List<Row> rows = List.of(
                row("111111111", 10.0, 15.0, false, false),
                row("111111111", 20.0, 30.0, true,  false),   // loitering=true
                row("111111111", 15.0, 60.0, false, true),    // inRestrictedZone=true
                row("222222222",  5.0, 10.0, false, false),
                row("222222222", 25.0, 55.0, false, true)     // inRestrictedZone=true
        );
        writeFixtureParquet(tempDir, rows);

        DailyVesselAggregatesJob.run(spark, cfg(tempDir, h2Url("daily1")));

        try (Connection c = DriverManager.getConnection(h2Url("daily1"), "sa", "");
             Statement  s = c.createStatement();
             ResultSet  r = s.executeQuery(
                     "SELECT mmsi, event_count, avg_speed_kn, max_speed_kn, " +
                     "avg_risk_score, restricted_count, loitering_count " +
                     "FROM vessel_daily_stats ORDER BY mmsi")) {

            // Collect into map for easy keyed assertions.
            Map<String, ResultRow> byMmsi = new LinkedHashMap<>();
            while (r.next()) byMmsi.put(r.getString("mmsi"), new ResultRow(r));

            // ── Vessel A ──────────────────────────────────────────────────────
            assertThat(byMmsi).containsKey("111111111");
            ResultRow a = byMmsi.get("111111111");
            assertThat(a.eventCount)      .isEqualTo(3L);
            assertThat(a.avgSpeed)        .isCloseTo(15.0,  within(0.01));
            assertThat(a.maxSpeed)        .isCloseTo(20.0,  within(0.01));
            // avg_risk_score = (15 + 30 + 60) / 3 = 35.0
            assertThat(a.avgRisk)         .isCloseTo(35.0,  within(0.01));
            // exactly 1 of 3 reports has inRestrictedZone=true
            assertThat(a.restrictedCount) .isEqualTo(1L);
            // exactly 1 of 3 reports has loitering=true
            assertThat(a.loiteringCount)  .isEqualTo(1L);

            // ── Vessel B ──────────────────────────────────────────────────────
            assertThat(byMmsi).containsKey("222222222");
            ResultRow b = byMmsi.get("222222222");
            assertThat(b.eventCount)      .isEqualTo(2L);
            // avg speed = (5 + 25) / 2 = 15
            assertThat(b.avgSpeed)        .isCloseTo(15.0,  within(0.01));
            assertThat(b.maxSpeed)        .isCloseTo(25.0,  within(0.01));
            assertThat(b.restrictedCount) .isEqualTo(1L);
            assertThat(b.loiteringCount)  .isEqualTo(0L);
        }
    }

    @Test
    @Order(2)
    @DisplayName("DailyVesselAggregatesJob — date filter excludes rows from other partitions")
    void dailyAggregates_dateFilter_excludesWrongDates(@TempDir Path tempDir) throws Exception {
        // Fixture only contains BATCH_DATE rows; verify exactly 1 MMSI is aggregated.
        writeFixtureParquet(tempDir, List.of(row("333333333", 12.0, 20.0, false, false)));

        DailyVesselAggregatesJob.run(spark, cfg(tempDir, h2Url("daily2")));

        try (Connection c = DriverManager.getConnection(h2Url("daily2"), "sa", "");
             Statement  s = c.createStatement();
             ResultSet  r = s.executeQuery("SELECT COUNT(*) FROM vessel_daily_stats")) {
            r.next();
            assertThat(r.getLong(1)).isEqualTo(1L);
        }
    }

    @Test
    @Order(3)
    @DisplayName("DailyVesselAggregatesJob — re-running the job is idempotent (SaveMode.Overwrite)")
    void dailyAggregates_rerun_isIdempotent(@TempDir Path tempDir) throws Exception {
        writeFixtureParquet(tempDir, List.of(row("444444444", 8.0, 10.0, false, false)));
        JobConfig cfg = cfg(tempDir, h2Url("daily3"));

        // Run twice — row count must remain 1 (Overwrite, not Append).
        DailyVesselAggregatesJob.run(spark, cfg);
        DailyVesselAggregatesJob.run(spark, cfg);

        try (Connection c = DriverManager.getConnection(h2Url("daily3"), "sa", "");
             Statement  s = c.createStatement();
             ResultSet  r = s.executeQuery(
                     "SELECT COUNT(*) FROM vessel_daily_stats WHERE mmsi='444444444'")) {
            r.next();
            assertThat(r.getLong(1)).isEqualTo(1L);
        }
    }

    // ── RiskRollupJob ─────────────────────────────────────────────────────────

    @Test
    @Order(4)
    @DisplayName("RiskRollupJob — expr(approx_percentile) executes without AnalysisException")
    void riskRollup_percentiles_computedWithoutCallUDF(@TempDir Path tempDir) throws Exception {
        // 10 events with evenly-spaced risk scores: 10, 20, …, 100.
        // Theoretical p50 = 55, p95 = 95 (at accuracy=100, ~1% relative error).
        List<Row> rows = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            rows.add(row("555555555", 10.0, i * 10.0, false, false));
        }
        writeFixtureParquet(tempDir, rows);
        JobConfig cfg = cfg(tempDir, h2Url("risk1"));

        // If callUDF("approx_percentile",...) were used instead of expr(...),
        // an AnalysisException would be thrown here at plan-analysis time,
        // before any data is read, because approx_percentile is not a registered UDF.
        assertThatCode(() -> RiskRollupJob.run(spark, cfg)).doesNotThrowAnyException();

        try (Connection c = DriverManager.getConnection(h2Url("risk1"), "sa", "");
             Statement  s = c.createStatement();
             ResultSet  r = s.executeQuery(
                     "SELECT p50_risk, p95_risk, sample_count " +
                     "FROM vessel_risk_percentiles WHERE mmsi='555555555'")) {

            assertThat(r.next()).isTrue();
            double p50 = r.getDouble("p50_risk");
            double p95 = r.getDouble("p95_risk");
            long   cnt = r.getLong("sample_count");

            assertThat(cnt).isEqualTo(10L);
            // Generous bounds: approx_percentile accuracy=100 gives ~1% relative error.
            assertThat(p50).isBetween(45.0, 65.0);
            assertThat(p95).isBetween(85.0, 100.0);
            // p95 must always be ≥ p50 — fundamental percentile invariant.
            assertThat(p95).isGreaterThanOrEqualTo(p50);
        }
    }

    @Test
    @Order(5)
    @DisplayName("RiskRollupJob — single event produces p50 == p95 == that event's riskScore")
    void riskRollup_singleEvent_p50EqualsP95(@TempDir Path tempDir) throws Exception {
        writeFixtureParquet(tempDir, List.of(row("666666666", 5.0, 75.0, false, false)));
        RiskRollupJob.run(spark, cfg(tempDir, h2Url("risk2")));

        try (Connection c = DriverManager.getConnection(h2Url("risk2"), "sa", "");
             Statement  s = c.createStatement();
             ResultSet  r = s.executeQuery(
                     "SELECT p50_risk, p95_risk FROM vessel_risk_percentiles " +
                     "WHERE mmsi='666666666'")) {
            assertThat(r.next()).isTrue();
            // Both percentiles of a single-element distribution equal that element.
            assertThat(r.getDouble("p50_risk")).isCloseTo(75.0, within(1.0));
            assertThat(r.getDouble("p95_risk")).isCloseTo(75.0, within(1.0));
        }
    }

    // ── LoiteringHotspotJob ───────────────────────────────────────────────────

    @Test
    @Order(6)
    @DisplayName("LoiteringHotspotJob — positions snapped to 0.1° grid cells with correct counts")
    void loiteringHotspot_gridSnapping_correct(@TempDir Path tempDir) throws Exception {
        // Three events in cell (30.0, -89.1) and one event in adjacent cell (30.1, -89.1).
        // The SW corner of a 0.1° cell is floor(coord / 0.1) * 0.1.
        //   lat=30.05 → floor(30.05/0.1)*0.1 = floor(300.5)*0.1 = 300*0.1 = 30.0
        //   lon=-89.05 → floor(-89.05/0.1)*0.1 = floor(-890.5)*0.1 = -891*0.1 = -89.1
        List<Row> rows = List.of(
                row("777777777", 30.05, -89.05, 0.5, 10.0, true, false),
                row("777777777", 30.07, -89.08, 0.3, 10.0, true, false),
                row("888888888", 30.02, -89.02, 0.4, 10.0, true, false),
                row("999999999", 30.15, -89.05, 0.2, 10.0, true, false)  // adjacent cell
        );
        writeFixtureParquet(tempDir, rows);
        LoiteringHotspotJob.run(spark, cfg(tempDir, h2Url("hotspot1")));

        try (Connection c = DriverManager.getConnection(h2Url("hotspot1"), "sa", "");
             Statement  s = c.createStatement();
             ResultSet  r = s.executeQuery(
                     "SELECT cell_lat, cell_lon, event_count, vessel_count " +
                     "FROM loitering_hotspots ORDER BY event_count DESC")) {

            // First result: dominant cell — 3 events, 2 distinct vessels.
            assertThat(r.next()).isTrue();
            assertThat(r.getLong("event_count")) .isEqualTo(3L);
            assertThat(r.getLong("vessel_count")).isEqualTo(2L);
            assertThat(r.getDouble("cell_lat"))  .isCloseTo(30.0,  within(0.0001));
            assertThat(r.getDouble("cell_lon"))  .isCloseTo(-89.1, within(0.0001));

            // Second result: adjacent cell — 1 event, 1 vessel.
            assertThat(r.next()).isTrue();
            assertThat(r.getLong("event_count")) .isEqualTo(1L);
            assertThat(r.getDouble("cell_lat"))  .isCloseTo(30.1,  within(0.0001));
        }
    }

    @Test
    @Order(7)
    @DisplayName("LoiteringHotspotJob — no loitering events skips the JDBC write entirely")
    void loiteringHotspot_noLoiteringEvents_skipsWrite(@TempDir Path tempDir) throws Exception {
        // All events have loitering=false — run() should return early without writing.
        writeFixtureParquet(tempDir, List.of(
                row("111222333", 12.0, 10.0, false, false),
                row("444555666", 14.0, 20.0, false, false)
        ));
        LoiteringHotspotJob.run(spark, cfg(tempDir, h2Url("hotspot2")));

        // The H2 database was created but no table should have been written.
        try (Connection c = DriverManager.getConnection(h2Url("hotspot2"), "sa", "")) {
            DatabaseMetaData meta = c.getMetaData();
            try (ResultSet tables = meta.getTables(null, null, "LOITERING_HOTSPOTS", null)) {
                assertThat(tables.next())
                        .as("loitering_hotspots table must not exist when no loitering events present")
                        .isFalse();
            }
        }
    }

    @Test
    @Order(8)
    @DisplayName("LoiteringHotspotJob — output capped at TOP_N (50) rows when input exceeds that")
    void loiteringHotspot_topNLimit_atMost50Rows(@TempDir Path tempDir) throws Exception {
        // 60 events in 60 distinct 0.1° cells (one event per cell) → should yield ≤ 50 rows.
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            // Each latitude step of 0.1° produces a distinct grid cell.
            double lat = i * 0.1;
            String mmsi = String.format("%09d", 100_000_000 + i);
            rows.add(row(mmsi, lat, 0.05, 0.5, 10.0, true, false));
        }
        writeFixtureParquet(tempDir, rows);
        LoiteringHotspotJob.run(spark, cfg(tempDir, h2Url("hotspot3")));

        try (Connection c = DriverManager.getConnection(h2Url("hotspot3"), "sa", "");
             Statement  s = c.createStatement();
             ResultSet  r = s.executeQuery("SELECT COUNT(*) FROM loitering_hotspots")) {
            r.next();
            assertThat(r.getLong(1))
                    .as("output must be capped at the TOP_N=50 limit defined in LoiteringHotspotJob")
                    .isLessThanOrEqualTo(50L);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /** Value-object for reading aggregated result rows from JDBC ResultSet. */
    static class ResultRow {
        final long   eventCount;
        final double avgSpeed;
        final double maxSpeed;
        final double avgRisk;
        final long   restrictedCount;
        final long   loiteringCount;

        ResultRow(ResultSet rs) throws SQLException {
            eventCount      = rs.getLong("event_count");
            avgSpeed        = rs.getDouble("avg_speed_kn");
            maxSpeed        = rs.getDouble("max_speed_kn");
            avgRisk         = rs.getDouble("avg_risk_score");
            restrictedCount = rs.getLong("restricted_count");
            loiteringCount  = rs.getLong("loitering_count");
        }
    }
}
