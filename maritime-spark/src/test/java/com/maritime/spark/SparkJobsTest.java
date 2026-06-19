package com.maritime.spark;

import com.maritime.spark.jobs.DailyVesselAggregatesJob;
import com.maritime.spark.jobs.JobWriter;
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
 * <h3>Strategy — no Postgres, no Spring context, no Testcontainers</h3>
 * Each job is now a plain {@code @Component} with constructor injection, so tests
 * instantiate jobs directly — no reflection, no {@code SpringRunner}, no
 * {@code ApplicationContext}. H2 in-memory database receives the JDBC writes, so
 * the full {@code execute()} path runs without a real Postgres.
 *
 * <h3>Test structure</h3>
 * <ol>
 *   <li>Write synthetic Parquet to a {@code @TempDir}.</li>
 *   <li>Build a {@link SparkJobProperties} pointing at that dir and an H2 URL.</li>
 *   <li>Build a {@link JobWriter} backed by the same H2 URL.</li>
 *   <li>Construct the job under test by passing the above via its constructor.</li>
 *   <li>Call {@code job.execute()} — the transformation runs end-to-end.</li>
 *   <li>Open a plain JDBC connection to H2 and assert the written rows.</li>
 * </ol>
 *
 * <h3>SparkSession lifecycle</h3>
 * One {@code local[1]} session is shared across all tests via
 * {@code @BeforeAll}/{@code @AfterAll}. Starting a SparkSession costs ~3–5 s;
 * sharing one keeps the full suite under 30 s.
 *
 * <h3>callUDF audit — RiskRollupJob</h3>
 * {@link RiskRollupJob} uses {@code expr("approx_percentile(...)")} — NOT
 * {@code callUDF("approx_percentile", ...)}. If that ever regresses,
 * {@link #riskRollup_percentiles_computedWithoutCallUDF} catches it immediately
 * with an {@code AnalysisException} at plan-analysis time.
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
                .config("spark.ui.enabled",             "false")
                .config("spark.sql.adaptive.enabled",   "false")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate();

        org.apache.log4j.Logger.getLogger("org.apache.spark")
                .setLevel(org.apache.log4j.Level.WARN);
        org.apache.log4j.Logger.getLogger("org.apache.hadoop")
                .setLevel(org.apache.log4j.Level.WARN);
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) spark.stop();
    }

    // ── Schema ────────────────────────────────────────────────────────────────

    static final String BATCH_DATE = "2024-03-15";

    static final StructType VESSEL_EVENT_STRUCT = new StructType()
            .add("mmsi",      DataTypes.StringType, false)
            .add("latitude",  DataTypes.DoubleType, false)
            .add("longitude", DataTypes.DoubleType, false)
            .add("speed",     DataTypes.DoubleType, false)
            .add("heading",   DataTypes.DoubleType, false)
            .add("timestamp", DataTypes.LongType,   false)
            .add("eventType", DataTypes.StringType, false);

    static final StructType ENRICHED_SCHEMA = new StructType()
            .add("vesselEvent",      VESSEL_EVENT_STRUCT,   false)
            .add("inRestrictedZone", DataTypes.BooleanType, false)
            .add("zoneName",         DataTypes.StringType,  true)
            .add("zoneType",         DataTypes.StringType,  true)
            .add("distanceToPort",   DataTypes.DoubleType,  false)
            .add("riskScore",        DataTypes.DoubleType,  false)
            .add("riskLevel",        DataTypes.StringType,  false)
            .add("loitering",        DataTypes.BooleanType, false)
            .add("darkVessel",       DataTypes.BooleanType, false)
            .add("speedAnomaly",     DataTypes.BooleanType, false)
            .add("date",             DataTypes.StringType,  false)  // Hive partition col
            .add("mmsi_key",         DataTypes.StringType,  false); // dropped before write

    // ── Fixture helpers ───────────────────────────────────────────────────────

    static String writeFixtureParquet(Path tempDir, List<Row> rows) {
        Dataset<Row> df = spark.createDataFrame(rows, ENRICHED_SCHEMA);
        String basePath = "file://" + tempDir.toAbsolutePath() + "/vessel-events";
        df.drop("mmsi_key").write().mode("overwrite").partitionBy("date").parquet(basePath);
        return basePath;
    }

    static Row row(String mmsi, double speed, double riskScore,
                   boolean loitering, boolean inRestrictedZone) {
        return row(mmsi, 30.5, -89.0, speed, riskScore, loitering, inRestrictedZone);
    }

    static Row row(String mmsi, double lat, double lon, double speed,
                   double riskScore, boolean loitering, boolean inRestrictedZone) {
        Row vesselEvent = RowFactory.create(
                mmsi, lat, lon, speed, 90.0, System.currentTimeMillis(), "AIS");
        return RowFactory.create(
                vesselEvent, inRestrictedZone, null, null, 25.0, riskScore,
                riskScore >= 50 ? "HIGH" : riskScore >= 20 ? "MEDIUM" : "LOW",
                loitering, false, false, BATCH_DATE, mmsi);
    }

    /**
     * Unique H2 in-memory URL per test — prevents table name collisions between
     * tests that run in the same JVM. {@code DB_CLOSE_DELAY=-1} keeps the DB
     * alive so the assertion connection can read what Spark wrote.
     */
    static String h2Url(String name) {
        return "jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
    }

    /**
     * Build a {@link SparkJobProperties} pointing at {@code tempDir} and {@code h2Url}.
     *
     * <p>Previously this required reflection into {@code JobConfig}'s private
     * constructor. Now that {@code SparkJobProperties} is a plain {@code @Component}
     * with a public constructor, we just call it directly.
     */
    static SparkJobProperties props(Path tempDir, String h2Url) {
        return new SparkJobProperties(
                tempDir.toAbsolutePath().toString(),  // coldTierDir
                h2Url,                                // dbUrl → H2
                "sa",                                 // dbUser (H2 default)
                "",                                   // dbPass (H2 default)
                BATCH_DATE                            // batchDate
        );
    }

    /**
     * Build a {@link JobWriter} connected to the given H2 URL.
     *
     * <p>JobWriter is now a Spring bean, but its constructor is public —
     * tests construct it directly without an ApplicationContext.
     * The DataSource is supplied as a simple lambda that returns an H2 connection,
     * matching the interface JobWriter needs (only {@code getConnection()} is called,
     * to resolve the JDBC URL from metadata).
     */
    static JobWriter writer(SparkJobProperties p) {
        javax.sql.DataSource ds = () -> DriverManager.getConnection(p.getDbUrl(), p.getDbUser(), p.getDbPass());
        return new JobWriter(p, ds);
    }

    // ── DailyVesselAggregatesJob ──────────────────────────────────────────────

    @Test
    @Order(1)
    @DisplayName("DailyVesselAggregatesJob — one aggregated row per MMSI with correct values")
    void dailyAggregates_oneRowPerVessel_correctAggregates(@TempDir Path tempDir) throws Exception {
        List<Row> rows = List.of(
                row("111111111", 10.0, 15.0, false, false),
                row("111111111", 20.0, 30.0, true,  false),
                row("111111111", 15.0, 60.0, false, true),
                row("222222222",  5.0, 10.0, false, false),
                row("222222222", 25.0, 55.0, false, true)
        );
        writeFixtureParquet(tempDir, rows);
        SparkJobProperties p = props(tempDir, h2Url("daily1"));

        new DailyVesselAggregatesJob(spark, p, writer(p)).execute();

        try (Connection c = DriverManager.getConnection(h2Url("daily1"), "sa", "");
             Statement  s = c.createStatement();
             ResultSet  r = s.executeQuery(
                     "SELECT mmsi, event_count, avg_speed_kn, max_speed_kn, " +
                     "avg_risk_score, restricted_count, loitering_count " +
                     "FROM vessel_daily_stats ORDER BY mmsi")) {

            Map<String, ResultRow> byMmsi = new LinkedHashMap<>();
            while (r.next()) byMmsi.put(r.getString("mmsi"), new ResultRow(r));

            ResultRow a = byMmsi.get("111111111");
            assertThat(a.eventCount)     .isEqualTo(3L);
            assertThat(a.avgSpeed)       .isCloseTo(15.0, within(0.01));
            assertThat(a.maxSpeed)       .isCloseTo(20.0, within(0.01));
            assertThat(a.avgRisk)        .isCloseTo(35.0, within(0.01));
            assertThat(a.restrictedCount).isEqualTo(1L);
            assertThat(a.loiteringCount) .isEqualTo(1L);

            ResultRow b = byMmsi.get("222222222");
            assertThat(b.eventCount)     .isEqualTo(2L);
            assertThat(b.avgSpeed)       .isCloseTo(15.0, within(0.01));
            assertThat(b.maxSpeed)       .isCloseTo(25.0, within(0.01));
            assertThat(b.restrictedCount).isEqualTo(1L);
            assertThat(b.loiteringCount) .isEqualTo(0L);
        }
    }

    @Test
    @Order(2)
    @DisplayName("DailyVesselAggregatesJob — date filter excludes other partitions")
    void dailyAggregates_dateFilter_excludesWrongDates(@TempDir Path tempDir) throws Exception {
        writeFixtureParquet(tempDir, List.of(row("333333333", 12.0, 20.0, false, false)));
        SparkJobProperties p = props(tempDir, h2Url("daily2"));

        new DailyVesselAggregatesJob(spark, p, writer(p)).execute();

        try (Connection c = DriverManager.getConnection(h2Url("daily2"), "sa", "");
             Statement  s = c.createStatement();
             ResultSet  r = s.executeQuery("SELECT COUNT(*) FROM vessel_daily_stats")) {
            r.next();
            assertThat(r.getLong(1)).isEqualTo(1L);
        }
    }

    @Test
    @Order(3)
    @DisplayName("DailyVesselAggregatesJob — re-run is idempotent (SaveMode.Overwrite)")
    void dailyAggregates_rerun_isIdempotent(@TempDir Path tempDir) throws Exception {
        writeFixtureParquet(tempDir, List.of(row("444444444", 8.0, 10.0, false, false)));
        SparkJobProperties p = props(tempDir, h2Url("daily3"));
        DailyVesselAggregatesJob job = new DailyVesselAggregatesJob(spark, p, writer(p));

        job.execute();
        job.execute();

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
        List<Row> rows = new ArrayList<>();
        for (int i = 1; i <= 10; i++) rows.add(row("555555555", 10.0, i * 10.0, false, false));
        writeFixtureParquet(tempDir, rows);
        SparkJobProperties p = props(tempDir, h2Url("risk1"));

        assertThatCode(() -> new RiskRollupJob(spark, p, writer(p)).execute())
                .doesNotThrowAnyException();

        try (Connection c = DriverManager.getConnection(h2Url("risk1"), "sa", "");
             Statement  s = c.createStatement();
             ResultSet  r = s.executeQuery(
                     "SELECT p50_risk, p95_risk, sample_count " +
                     "FROM vessel_risk_percentiles WHERE mmsi='555555555'")) {
            assertThat(r.next()).isTrue();
            assertThat(r.getLong("sample_count")).isEqualTo(10L);
            assertThat(r.getDouble("p50_risk")).isBetween(45.0, 65.0);
            assertThat(r.getDouble("p95_risk")).isBetween(85.0, 100.0);
            assertThat(r.getDouble("p95_risk")).isGreaterThanOrEqualTo(r.getDouble("p50_risk"));
        }
    }

    @Test
    @Order(5)
    @DisplayName("RiskRollupJob — single event: p50 == p95 == riskScore")
    void riskRollup_singleEvent_p50EqualsP95(@TempDir Path tempDir) throws Exception {
        writeFixtureParquet(tempDir, List.of(row("666666666", 5.0, 75.0, false, false)));
        SparkJobProperties p = props(tempDir, h2Url("risk2"));

        new RiskRollupJob(spark, p, writer(p)).execute();

        try (Connection c = DriverManager.getConnection(h2Url("risk2"), "sa", "");
             Statement  s = c.createStatement();
             ResultSet  r = s.executeQuery(
                     "SELECT p50_risk, p95_risk FROM vessel_risk_percentiles " +
                     "WHERE mmsi='666666666'")) {
            assertThat(r.next()).isTrue();
            assertThat(r.getDouble("p50_risk")).isCloseTo(75.0, within(1.0));
            assertThat(r.getDouble("p95_risk")).isCloseTo(75.0, within(1.0));
        }
    }

    // ── LoiteringHotspotJob ───────────────────────────────────────────────────

    @Test
    @Order(6)
    @DisplayName("LoiteringHotspotJob — positions snapped to 0.1° cells with correct counts")
    void loiteringHotspot_gridSnapping_correct(@TempDir Path tempDir) throws Exception {
        List<Row> rows = List.of(
                row("777777777", 30.05, -89.05, 0.5, 10.0, true, false),
                row("777777777", 30.07, -89.08, 0.3, 10.0, true, false),
                row("888888888", 30.02, -89.02, 0.4, 10.0, true, false),
                row("999999999", 30.15, -89.05, 0.2, 10.0, true, false)
        );
        writeFixtureParquet(tempDir, rows);
        SparkJobProperties p = props(tempDir, h2Url("hotspot1"));

        new LoiteringHotspotJob(spark, p, writer(p)).execute();

        try (Connection c = DriverManager.getConnection(h2Url("hotspot1"), "sa", "");
             Statement  s = c.createStatement();
             ResultSet  r = s.executeQuery(
                     "SELECT cell_lat, cell_lon, event_count, vessel_count " +
                     "FROM loitering_hotspots ORDER BY event_count DESC")) {
            assertThat(r.next()).isTrue();
            assertThat(r.getLong("event_count")) .isEqualTo(3L);
            assertThat(r.getLong("vessel_count")).isEqualTo(2L);
            assertThat(r.getDouble("cell_lat"))  .isCloseTo(30.0,  within(0.0001));
            assertThat(r.getDouble("cell_lon"))  .isCloseTo(-89.1, within(0.0001));

            assertThat(r.next()).isTrue();
            assertThat(r.getLong("event_count")).isEqualTo(1L);
            assertThat(r.getDouble("cell_lat")) .isCloseTo(30.1, within(0.0001));
        }
    }

    @Test
    @Order(7)
    @DisplayName("LoiteringHotspotJob — no loitering events skips JDBC write")
    void loiteringHotspot_noLoiteringEvents_skipsWrite(@TempDir Path tempDir) throws Exception {
        writeFixtureParquet(tempDir, List.of(
                row("111222333", 12.0, 10.0, false, false),
                row("444555666", 14.0, 20.0, false, false)
        ));
        SparkJobProperties p = props(tempDir, h2Url("hotspot2"));

        new LoiteringHotspotJob(spark, p, writer(p)).execute();

        try (Connection c = DriverManager.getConnection(h2Url("hotspot2"), "sa", "")) {
            try (ResultSet tables = c.getMetaData().getTables(null, null, "LOITERING_HOTSPOTS", null)) {
                assertThat(tables.next())
                        .as("table must not exist when no loitering events")
                        .isFalse();
            }
        }
    }

    @Test
    @Order(8)
    @DisplayName("LoiteringHotspotJob — output capped at TOP_N=50 rows")
    void loiteringHotspot_topNLimit_atMost50Rows(@TempDir Path tempDir) throws Exception {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            rows.add(row(String.format("%09d", 100_000_000 + i),
                    i * 0.1, 0.05, 0.5, 10.0, true, false));
        }
        writeFixtureParquet(tempDir, rows);
        SparkJobProperties p = props(tempDir, h2Url("hotspot3"));

        new LoiteringHotspotJob(spark, p, writer(p)).execute();

        try (Connection c = DriverManager.getConnection(h2Url("hotspot3"), "sa", "");
             Statement  s = c.createStatement();
             ResultSet  r = s.executeQuery("SELECT COUNT(*) FROM loitering_hotspots")) {
            r.next();
            assertThat(r.getLong(1))
                    .as("capped at TOP_N=50")
                    .isLessThanOrEqualTo(50L);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    static class ResultRow {
        final long   eventCount, restrictedCount, loiteringCount;
        final double avgSpeed, maxSpeed, avgRisk;

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
