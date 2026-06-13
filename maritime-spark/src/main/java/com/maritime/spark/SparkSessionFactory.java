package com.maritime.spark;

import org.apache.spark.sql.SparkSession;

/**
 * Factory for {@link SparkSession} instances configured for the maritime platform.
 *
 * <h3>Why a factory and not a singleton?</h3>
 * Each Spark job is a standalone JVM process. SparkSession is already a per-JVM
 * singleton internally (SparkSession.builder().getOrCreate() reuses an existing
 * session). The factory exists to centralise the Parquet/Avro configuration that
 * every job needs, and to allow tests to override {@code master("local[*]")}
 * without duplicating the config block.
 *
 * <h3>Cold tier on the local filesystem</h3>
 * The storage service writes Parquet to a local directory
 * ({@code <COLD_TIER_DIR>/vessel-events/date=/mmsi=/}). Jobs read it via a plain
 * {@code file://} URI handled by Hadoop's built-in {@code LocalFileSystem} — no
 * S3A connector, no LocalStack, no AWS credentials. Partition discovery still
 * exposes the Hive-style {@code date=} / {@code mmsi=} segments as DataFrame
 * columns, so partition pruning works exactly as it did against S3.
 */
public final class SparkSessionFactory {

    private SparkSessionFactory() {}

    /**
     * Build a SparkSession for production use (submitted via spark-submit).
     * Spark's cluster manager provides the master URL via {@code --master} flag.
     *
     * @param appName human-readable job name shown in the Spark UI
     */
    public static SparkSession create(String appName) {
        return baseBuilder(appName).getOrCreate();
    }

    /**
     * Build a local SparkSession for direct execution (IDE / mvn exec:java with
     * the {@code local} Maven profile).  {@code master("local[*]")} runs the job
     * in the current JVM using all available CPU cores.
     */
    public static SparkSession createLocal(String appName) {
        return baseBuilder(appName)
                .master("local[*]")
                .getOrCreate();
    }

    private static SparkSession.Builder baseBuilder(String appName) {
        return SparkSession.builder()
                .appName(appName)

                // ── Parquet / Avro ────────────────────────────────────────────
                // Use the Avro-aware Parquet reader so EnrichedVesselEvent schema
                // is used for column mapping when reading the cold tier.
                .config("spark.sql.parquet.enableVectorizedReader", "false")

                // ── Stability / logging ───────────────────────────────────────
                // Suppress the noisy Spark SQL adaptive query planning logs in
                // local mode so job progress is readable.
                .config("spark.sql.adaptive.enabled",    "true")
                .config("spark.ui.enabled",              "false");
    }
}
