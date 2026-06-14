package com.maritime.spark.jobs;

import com.maritime.spark.JobConfig;
import com.maritime.spark.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.*;

/**
 * Batch job: loitering hotspot grid mining.
 *
 * <h3>Algorithm</h3>
 * Snap each loitering event to a 0.1° × 0.1° grid cell (≈ 11 km at the equator):
 * <pre>
 *   cell_lat = floor(latitude  / 0.1) * 0.1
 *   cell_lon = floor(longitude / 0.1) * 0.1
 * </pre>
 * Count loitering events per cell per day. Write the top-50 cells (by event
 * count) to the {@code loitering_hotspots} table in PostGIS.
 *
 * <h3>Why 0.1°?</h3>
 * 0.1° ≈ 11 km side length — coarse enough to aggregate nearby reports into
 * meaningful clusters, fine enough to distinguish harbour approaches from open
 * anchorages. This is a tunable parameter; adjust for the target area size.
 *
 * <h3>Output table: loitering_hotspots</h3>
 * <pre>
 * date         DATE
 * cell_lat     DOUBLE   — south-west corner of the grid cell
 * cell_lon     DOUBLE   — south-west corner of the grid cell
 * event_count  BIGINT
 * vessel_count BIGINT   — distinct vessels that loitered in this cell
 * </pre>
 *
 * <h3>Downstream use</h3>
 * The API service serves this table as a GeoJSON feature collection (Phase 8 /
 * MCP tool). Grafana can also query it directly via the PostgreSQL data source.
 */
public class LoiteringHotspotJob {

    static final String TARGET_TABLE = "loitering_hotspots";
    private static final Logger log = LoggerFactory.getLogger(LoiteringHotspotJob.class);
    /**
     * Grid cell side length in degrees.
     */
    private static final double GRID_DEG = 0.1;

    /**
     * Number of top cells to retain in the output table.
     */
    private static final int TOP_N = 50;

    public static void main(String[] args) {
        JobConfig cfg = JobConfig.fromEnv();
        log.info("LoiteringHotspotJob starting — date={}", cfg.batchDate);

        SparkSession spark = SparkSessionFactory.createLocal("LoiteringHotspot-" + cfg.batchDate);

        run(spark, cfg);
        spark.stop();
        log.info("LoiteringHotspotJob complete");
    }

    public static void run(SparkSession spark, JobConfig cfg) {
        Dataset<Row> raw = spark.read()
                .format("parquet")
                .option("mergeSchema", "false")
                .load(cfg.parquetInputPath());

        // ── Filter to loitering events only ───────────────────────────────────
        Dataset<Row> loitering = raw
                .filter(col("date").equalTo(cfg.batchDate))
                .filter(col("loitering").equalTo(true));

        long loiteringCount = loitering.count();
        if (loiteringCount == 0) {
            log.info("No loitering events for date={}; skipping hotspot write", cfg.batchDate);
            return;
        }
        log.info("Processing {} loitering events", loiteringCount);

        // ── Snap to grid and aggregate ────────────────────────────────────────
        // floor(x / GRID_DEG) * GRID_DEG snaps to the south-west corner of the
        // containing cell. We round to 6 dp to avoid floating-point representation
        // noise accumulating across millions of rows.
        Dataset<Row> hotspots = loitering
                .withColumn("cell_lat",
                        round(floor(col("vesselEvent.latitude")
                                .divide(GRID_DEG)).multiply(GRID_DEG), 6))
                .withColumn("cell_lon",
                        round(floor(col("vesselEvent.longitude")
                                .divide(GRID_DEG)).multiply(GRID_DEG), 6))
                .groupBy(
                        lit(cfg.batchDate).cast("date").alias("date"),
                        col("cell_lat"),
                        col("cell_lon")
                )
                .agg(
                        count("*").alias("event_count"),
                        countDistinct("vesselEvent.mmsi").alias("vessel_count")
                )
                .orderBy(col("event_count").desc())
                .limit(TOP_N);

        DailyVesselAggregatesJob.writeToPostgres(hotspots, TARGET_TABLE, cfg);
        log.info("Wrote {} hotspot cells to {}", hotspots.count(), TARGET_TABLE);
    }
}
