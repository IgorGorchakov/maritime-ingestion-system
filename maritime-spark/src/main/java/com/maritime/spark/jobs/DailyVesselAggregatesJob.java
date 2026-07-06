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
 * writes one row per vessel per day to {@code vessel_daily_stats} in PostGIS.
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
 * This is the <em>batch layer</em>: exact aggregates over the complete immutable
 * cold tier, complementing the <em>speed layer</em> ({@code MaritimeTopology} in
 * {@code maritime-detection}). The API service merges both views.
 *
 * <h3>Partition pruning</h3>
 * {@code parquetInputPath()} already points at the {@code date=} partition for
 * the batch date. The explicit {@code .filter(col("date")...)} provides a
 * belt-and-suspenders predicate that Spark can push down to the scan level,
 * skipping any other date partitions in the same path.
 *
 * <h3>Idempotency</h3>
 * {@code SaveMode.Overwrite} with {@code truncate=true} — re-running for the
 * same date replaces rows without dropping the table.
 *
 * <h3>Spring lifecycle</h3>
 * {@code @Order(1)} — runs first among the three {@link ApplicationRunner} jobs.
 */
@Component
@Order(1)
public class DailyVesselAggregatesJob implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(DailyVesselAggregatesJob.class);
    public static final String TARGET_TABLE = "vessel_daily_stats";

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
                props.batchDate(), props.inputPath());
        execute();
        log.info("DailyVesselAggregatesJob complete");
    }

    /**
     * Core transformation — extracted for direct testability without an
     * {@link ApplicationArguments} instance.
     */
    public void execute() {
        Dataset<Row> raw = spark.read()
                .format("json")
                .option("multiLine", "false")
                .load(props.inputPath());

        Dataset<Row> aggregated = raw
                .filter(col("date").equalTo(props.batchDate()))
                .groupBy(
                        col("vesselEvent.mmsi").alias("mmsi"),
                        lit(props.batchDate()).cast("date").alias("date")
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

        // write() triggers the single Spark action for this job.
        // No count() call before or after — count() would re-execute the full
        // plan a second time (DataFrames are lazy), doubling execution cost.
        writer.write(aggregated, TARGET_TABLE);
        log.info("DailyVesselAggregatesJob wrote to {}", TARGET_TABLE);
    }
}
