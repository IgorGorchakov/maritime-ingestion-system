package com.maritime.spark;

import org.apache.spark.sql.SparkSession;

/**
 * Factory for {@link SparkSession} instances configured for the maritime platform.
 *
 * <h3>Why a factory and not a singleton?</h3>
 * Each Spark job is a standalone JVM process. SparkSession is already a per-JVM
 * singleton internally (SparkSession.builder().getOrCreate() reuses an existing
 * session). The factory exists to centralise the S3A/LocalStack configuration
 * that every job needs, and to allow tests to override {@code master("local[*]")}
 * without duplicating the Hadoop config block.
 *
 * <h3>S3A → LocalStack configuration</h3>
 * <ul>
 *   <li>{@code fs.s3a.endpoint} — points S3A at LocalStack instead of AWS.</li>
 *   <li>{@code fs.s3a.path.style.access=true} — LocalStack requires path-style
 *       ({@code http://localhost:4566/<bucket>/<key>}), not virtual-hosted-style
 *       ({@code http://<bucket>.s3.amazonaws.com/<key>}).</li>
 *   <li>{@code fs.s3a.impl} — registers the S3AFileSystem for s3a:// URIs.</li>
 *   <li>Magic committer disabled — LocalStack does not support the S3 multi-part
 *       upload abort that the magic committer relies on for atomic task commits.</li>
 * </ul>
 */
public final class SparkSessionFactory {

    private SparkSessionFactory() {}

    /**
     * Build a SparkSession for production use (submitted via spark-submit).
     * Spark's cluster manager provides the master URL via {@code --master} flag.
     *
     * @param appName  human-readable job name shown in the Spark UI
     * @param s3Endpoint LocalStack (or real AWS) S3 endpoint, e.g. {@code http://localhost:4566}
     * @param accessKey  AWS / LocalStack access key
     * @param secretKey  AWS / LocalStack secret key
     */
    public static SparkSession create(String appName,
                                      String s3Endpoint,
                                      String accessKey,
                                      String secretKey) {
        return baseBuilder(appName, s3Endpoint, accessKey, secretKey).getOrCreate();
    }

    /**
     * Build a local SparkSession for direct execution (IDE / mvn exec:java with
     * the {@code local} Maven profile).  {@code master("local[*]")} runs the job
     * in the current JVM using all available CPU cores.
     */
    public static SparkSession createLocal(String appName,
                                           String s3Endpoint,
                                           String accessKey,
                                           String secretKey) {
        return baseBuilder(appName, s3Endpoint, accessKey, secretKey)
                .master("local[*]")
                .getOrCreate();
    }

    private static SparkSession.Builder baseBuilder(String appName,
                                                     String s3Endpoint,
                                                     String accessKey,
                                                     String secretKey) {
        return SparkSession.builder()
                .appName(appName)

                // ── S3A / LocalStack ──────────────────────────────────────────
                .config("spark.hadoop.fs.s3a.endpoint",          s3Endpoint)
                .config("spark.hadoop.fs.s3a.access.key",        accessKey)
                .config("spark.hadoop.fs.s3a.secret.key",        secretKey)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl",
                        "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                // Disable magic committer — LocalStack does not support the
                // multi-part upload abort that it relies on.
                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
                .config("spark.sql.parquet.output.committer.class",
                        "org.apache.parquet.hadoop.ParquetOutputCommitter")

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
