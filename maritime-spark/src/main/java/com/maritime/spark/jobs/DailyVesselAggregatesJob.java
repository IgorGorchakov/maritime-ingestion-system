package com.maritime.spark.jobs;

import com.maritime.spark.JobConfig;
import com.maritime.spark.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.spark.sql.functions.*;

/**
 * Batch job: daily per-vessel aggregates from the Parquet cold tier.
 *
 * <h3>What it does</h3>
 * Reads all Parquet files for {@code BATCH_DATE}, groups by MMSI, and writes
 * one row per vessel per day to the {@code vessel_daily_stats} table in PostGIS.
 *
 * <h3>Output schema (vessel_daily_stats)</h3>
 * <pre>
 * mmsi              VARCHAR(9)     — vessel identifier
 * date              DATE           — partition date
 * event_count       BIGINT         — number of AIS reports received
 * avg_speed_kn      DOUBLE         — average speed over ground
 * max_speed_kn      DOUBLE         — maximum speed over ground
 * avg_risk_score    DOUBLE         — mean risk score across the day
 * restricted_count  BIGINT         — reports where inRestrictedZone=true
 * loitering_count   BIGINT         — reports where loitering=true
 * dark_vessel_count BIGINT         — reports where darkVessel=true
 * speed_anomaly_count BIGINT       — reports where speedAnomaly=true
 * </pre>
 *
 * <h3>Lambda architecture role</h3>
 * This job is the <em>batch layer</em>: it computes exact aggregates over the
 * complete immutable cold tier, complementing the <em>speed layer</em>
 * (MaritimeTopology) which produces approximate near-real-time detections.
 * The gateway serving layer merges both views.
 *
 * <h3>Parquet partitioning</h3>
 * The storage service writes {@code s3a://<bucket>/vessel-events/date=<date>/mmsi=<mmsi>/events.parquet}.
 * Spark's partition discovery reads the {@code date} and {@code mmsi} path
 * segments as virtual columns, so we can push a {@code WHERE date = ?} predicate
 * down to S3 and avoid reading unneeded partitions (partition pruning).
 *
 * <h3>Running locally</h3>
 * <pre>{@code
 * cd maritime-spark
 * mvn exec:java \
 *   -Plocal \
 *   -Dexec.mainClass=com.maritime.spark.jobs.DailyVesselAggregatesJob \
 *   -DBATCH_DATE=2024-01-15
 * }</pre>
 *
 * <h3>Running via spark-submit</h3>
 * <pre>{@code
 * spark-submit \
 *   --class com.maritime.spark.jobs.DailyVesselAggregatesJob \
 *   --master local[*] \
 *   target/maritime-spark-1.0.0-SNAPSHOT-shaded.jar
 * }</pre>
 */
public class DailyVesselAggregatesJob {

    private static final Logger log = LoggerFactory.getLogger(DailyVesselAggregatesJob.class);
    static final String TARGET_TABLE = "vessel_daily_stats";

    public static void main(String[] args) {
        JobConfig cfg = JobConfig.fromEnv();
        log.info("DailyVesselAggregatesJob starting — date={} input={}",
                cfg.batchDate, cfg.parquetInputPath());

        SparkSession spark = SparkSessionFactory.createLocal(
                "DailyVesselAggregates-" + cfg.batchDate,
                cfg.s3Endpoint, cfg.awsAccessKey, cfg.awsSecretKey);

        run(spark, cfg);
        spark.stop();
        log.info("DailyVesselAggregatesJob complete");
    }

    /**
     * Core logic extracted for testability — accepts an already-built SparkSession
     * so unit tests can pass an in-memory {@code SparkSession.master("local[1]")}.
     */
    static void run(SparkSession spark, JobConfig cfg) {
        // ── Read ──────────────────────────────────────────────────────────────
        // Spark auto-discovers the date= and mmsi= partition columns from the path.
        // We filter on date immediately so only one partition is loaded — this is
        // the key partition-pruning optimisation: O(1 day) not O(all time).
        Dataset<Row> raw = spark.read()
                .format("parquet")
                .option("mergeSchema", "false")   // schema is fixed; merging is slow
                .load(cfg.parquetInputPath());

        log.info("Loaded {} records for date={}", raw.count(), cfg.batchDate);

        // ── Transform ─────────────────────────────────────────────────────────
        // Parquet column names come from the Avro schema field names (camelCase).
        // The vesselEvent fields are nested under a struct; use dot notation.
        Dataset<Row> aggregated = raw
                .filter(col("date").equalTo(cfg.batchDate))   // belt-and-suspenders if path glob was broad
                .groupBy(
                        col("vesselEvent.mmsi").alias("mmsi"),
                        lit(cfg.batchDate).cast("date").alias("date")
                )
                .agg(
                        count("*").alias("event_count"),
                        avg("vesselEvent.speed").alias("avg_speed_kn"),
                        max("vesselEvent.speed").alias("max_speed_kn"),
                        avg("riskScore").alias("avg_risk_score"),
                        sum(col("inRestrictedZone").cast("int")).alias("restricted_count"),
                        sum(col("loitering").cast("int")).alias("loitering_count"),
                        sum(col("darkVessel").cast("int")).alias("dark_vessel_count"),
                        sum(col("speedAnomaly").cast("int")).alias("speed_anomaly_count")
                );

        // ── Load ──────────────────────────────────────────────────────────────
        writeToPostgres(aggregated, TARGET_TABLE, cfg);
        log.info("Wrote {} vessel aggregates to {}", aggregated.count(), TARGET_TABLE);
    }

    static void writeToPostgres(Dataset<Row> df, String table, JobConfig cfg) {
        Properties jdbcProps = new Properties();
        jdbcProps.setProperty("user",   cfg.dbUser);
        jdbcProps.setProperty("password", cfg.dbPass);
        jdbcProps.setProperty("driver", "org.postgresql.Driver");

        // Overwrite today's partition — re-running the job for the same date is
        // idempotent. Use Append in production if you want an audit trail of runs.
        df.write()
                .mode(SaveMode.Overwrite)
                .option("truncate", "true")     // reuse the table rather than DROP/CREATE
                .jdbc(cfg.dbUrl, table, jdbcProps);
    }
}
