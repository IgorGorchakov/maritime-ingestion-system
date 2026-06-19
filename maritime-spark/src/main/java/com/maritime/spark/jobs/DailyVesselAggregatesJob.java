package com.maritime.spark.jobs;

import com.maritime.spark.SparkJobProperties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.*;

/**
 * Batch job: daily per-vessel aggregates from the Parquet cold tier.
 *
 * <h3>What it does</h3>
 * Reads all Parquet files for {@code spark.job.batch-date}, groups by MMSI, and
 * writes one row per vessel per day to the {@code vessel_daily_stats} table in
 * PostGIS.
 *
 * <h3>Output schema (vessel_daily_stats)</h3>
 * <pre>
 * mmsi                VARCHAR(9)
 * date                DATE
 * event_count         BIGINT    — AIS reports received
 * avg_speed_kn        DOUBLE    — average SOG over the day
 * max_speed_kn        DOUBLE    — maximum SOG over the day
 * avg_risk_score      DOUBLE    — mean risk score
 * restricted_count    BIGINT    — reports where inRestrictedZone=true
 * loitering_count     BIGINT    — reports where loitering=true
 * dark_vessel_count   BIGINT    — reports where darkVessel=true
 * speed_anomaly_count BIGINT    — reports where speedAnomaly=true
 * </pre>
 *
 * <h3>Lambda architecture role</h3>
 * This job is the <em>batch layer</em>: it computes exact aggregates over the
 * complete immutable cold tier, complementing the <em>speed layer</em>
 * ({@code MaritimeTopology} in {@code maritime-detection}) which produces
 * approximate near-real-time detections. The API service serving layer merges
 * both views behind one contract.
 *
 * <h3>Partition pruning</h3>
 * {@code maritime-storage} writes Parquet to
 * {@code <coldTierDir>/vessel-events/date=<date>/mmsi=<mmsi>/<epochMs>.parquet}.
 * Spark's partition discovery reads the {@code date} path segment as a virtual
 * column, allowing a {@code WHERE date = ?} predicate to skip all other
 * date partitions at the filesystem level — O(1 day) not O(all time).
 *
 * <h3>Idempotency</h3>
 * Uses {@code SaveMode.Overwrite} with {@code truncate=true} — re-running for
 * the same date replaces the existing rows without dropping the table.
 *
 * <h3>Spring lifecycle</h3>
 * Implements {@link ApplicationRunner}: Spring calls {@link #run} after the
 * application context is fully initialised. {@code @Order(1)} ensures this job
 * runs first when multiple {@code ApplicationRunner} beans are present.
 */
@Component
@Order(1)
public class DailyVesselAggregatesJob implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(DailyVesselAggregatesJob.class);
    static final String TARGET_TABLE = "vessel_daily_stats";

    private final SparkSession       spark;
    private final SparkJobProperties props;
    private final JobWriter          writer;

    public DailyVesselAggregatesJob(SparkSession spark,
                                     SparkJobProperties props,
                                     JobWriter writer) {
        this.spark  = spark;
        this.props  = props;
        this.writer = writer;
    }

    @Override
    public void run(ApplicationArguments args) {
        log.info("DailyVesselAggregatesJob starting — date={} input={}",
                props.getBatchDate(), props.parquetInputPath());
        execute();
        log.info("DailyVesselAggregatesJob complete");
    }

    /**
     * Core transformation logic — extracted so it is independently testable
     * without an {@link ApplicationArguments} instance.
     */
    public void execute() {
        Dataset<Row> raw = spark.read()
                .format("parquet")
                .option("mergeSchema", "false")
                .load(props.parquetInputPath());

        log.info("Loaded {} records for date={}", raw.count(), props.getBatchDate());

        Dataset<Row> aggregated = raw
                // Belt-and-suspenders date filter — parquetInputPath() already
                // points at the date= partition, but an explicit filter here
                // enables Spark to apply the predicate at the scan level too.
                .filter(col("date").equalTo(props.getBatchDate()))
                .groupBy(
                        col("vesselEvent.mmsi").alias("mmsi"),
                        lit(props.getBatchDate()).cast("date").alias("date")
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

        writer.write(aggregated, TARGET_TABLE);
        log.info("Wrote {} vessel aggregates to {}", aggregated.count(), TARGET_TABLE);
    }
}
