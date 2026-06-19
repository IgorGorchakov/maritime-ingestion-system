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
 * Batch job: loitering hotspot grid mining.
 *
 * <h3>Algorithm</h3>
 * Snap each loitering event to a {@link #GRID_DEG} × {@link #GRID_DEG} grid cell
 * (≈ 11 km at the equator):
 * <pre>
 *   cell_lat = floor(latitude  / GRID_DEG) * GRID_DEG
 *   cell_lon = floor(longitude / GRID_DEG) * GRID_DEG
 * </pre>
 * Count loitering events per cell per day. Write the top-{@link #TOP_N} cells
 * (by event count) to {@code loitering_hotspots} in PostGIS.
 *
 * <h3>Why 0.1°?</h3>
 * ≈ 11 km side length — coarse enough to cluster nearby reports into meaningful
 * hotspots, fine enough to distinguish harbour approaches from open anchorages.
 *
 * <h3>Output table: loitering_hotspots</h3>
 * <pre>
 * date          DATE
 * cell_lat      DOUBLE   — south-west corner latitude of the grid cell
 * cell_lon      DOUBLE   — south-west corner longitude of the grid cell
 * event_count   BIGINT
 * vessel_count  BIGINT   — distinct vessels that loitered in this cell
 * </pre>
 *
 * <h3>Spring lifecycle</h3>
 * {@code @Order(3)} — runs after {@link RiskRollupJob} ({@code @Order(2)}).
 */
@Component
@Order(3)
public class LoiteringHotspotJob implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(LoiteringHotspotJob.class);
    static final String TARGET_TABLE = "loitering_hotspots";

    /** Grid cell side length in degrees. */
    static final double GRID_DEG = 0.1;

    /** Maximum number of cells written per run. */
    static final int TOP_N = 50;

    private final SparkSession       spark;
    private final SparkJobProperties props;
    private final JobWriter          writer;

    public LoiteringHotspotJob(SparkSession spark,
                                SparkJobProperties props,
                                JobWriter writer) {
        this.spark  = spark;
        this.props  = props;
        this.writer = writer;
    }

    @Override
    public void run(ApplicationArguments args) {
        log.info("LoiteringHotspotJob starting — date={}", props.getBatchDate());
        execute();
        log.info("LoiteringHotspotJob complete");
    }

    public void execute() {
        Dataset<Row> raw = spark.read()
                .format("parquet")
                .option("mergeSchema", "false")
                .load(props.parquetInputPath());

        Dataset<Row> loitering = raw
                .filter(col("date").equalTo(props.getBatchDate()))
                .filter(col("loitering").equalTo(true));

        long loiteringCount = loitering.count();
        if (loiteringCount == 0) {
            log.info("No loitering events for date={}; skipping hotspot write", props.getBatchDate());
            return;
        }
        log.info("Processing {} loitering events", loiteringCount);

        // floor(x / GRID_DEG) * GRID_DEG snaps to the south-west corner of the
        // containing cell. round(…, 6) suppresses floating-point noise that would
        // otherwise split a single logical cell into several distinct group keys.
        Dataset<Row> hotspots = loitering
                .withColumn("cell_lat",
                        round(floor(col("vesselEvent.latitude")
                                .divide(GRID_DEG)).multiply(GRID_DEG), 6))
                .withColumn("cell_lon",
                        round(floor(col("vesselEvent.longitude")
                                .divide(GRID_DEG)).multiply(GRID_DEG), 6))
                .groupBy(
                        lit(props.getBatchDate()).cast("date").alias("date"),
                        col("cell_lat"),
                        col("cell_lon")
                )
                .agg(
                        count("*").alias("event_count"),
                        countDistinct("vesselEvent.mmsi").alias("vessel_count")
                )
                .orderBy(col("event_count").desc())
                .limit(TOP_N);

        writer.write(hotspots, TARGET_TABLE);
        log.info("Wrote {} hotspot cells to {}", hotspots.count(), TARGET_TABLE);
    }
}
