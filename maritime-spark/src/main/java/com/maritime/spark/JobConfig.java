package com.maritime.spark;

/**
 * Runtime configuration for all Spark batch jobs.
 *
 * Values come from environment variables (production/CI) or fall back to
 * LocalStack defaults (local development). Using env vars (rather than a
 * properties file) keeps the jobs compatible with both {@code spark-submit}
 * and direct execution without requiring a config file on the classpath.
 *
 * <pre>
 * Variable               Default                        Description
 * ───────────────────────────────────────────────────────────────────────────
 * S3_ENDPOINT            http://localhost:4566          LocalStack / real S3
 * AWS_ACCESS_KEY_ID      test                           LocalStack dummy key
 * AWS_SECRET_ACCESS_KEY  test                           LocalStack dummy secret
 * S3_BUCKET              maritime-data                  S3 bucket for cold tier
 * DB_URL                 jdbc:postgresql://localhost:    PostGIS JDBC URL
 *                          5432/maritime
 * DB_USER                postgres
 * DB_PASS                postgres
 * BATCH_DATE             yesterday (ISO date)           Date partition to process
 * </pre>
 */
public final class JobConfig {

    public final String s3Endpoint;
    public final String awsAccessKey;
    public final String awsSecretKey;
    public final String s3Bucket;
    public final String dbUrl;
    public final String dbUser;
    public final String dbPass;
    /** ISO date (yyyy-MM-dd) of the partition to process. */
    public final String batchDate;

    private JobConfig(String s3Endpoint, String awsAccessKey, String awsSecretKey,
                      String s3Bucket, String dbUrl, String dbUser, String dbPass,
                      String batchDate) {
        this.s3Endpoint    = s3Endpoint;
        this.awsAccessKey  = awsAccessKey;
        this.awsSecretKey  = awsSecretKey;
        this.s3Bucket      = s3Bucket;
        this.dbUrl         = dbUrl;
        this.dbUser        = dbUser;
        this.dbPass        = dbPass;
        this.batchDate     = batchDate;
    }

    public static JobConfig fromEnv() {
        // Default batchDate = yesterday so a scheduled daily job always processes
        // the previous full day without requiring an explicit parameter.
        String yesterday = java.time.LocalDate.now()
                .minusDays(1)
                .toString();   // ISO-8601: yyyy-MM-dd

        return new JobConfig(
                env("S3_ENDPOINT",            "http://localhost:4566"),
                env("AWS_ACCESS_KEY_ID",       "test"),
                env("AWS_SECRET_ACCESS_KEY",   "test"),
                env("S3_BUCKET",               "maritime-data"),
                env("DB_URL",  "jdbc:postgresql://localhost:5432/maritime"),
                env("DB_USER", "postgres"),
                env("DB_PASS", "postgres"),
                env("BATCH_DATE", yesterday)
        );
    }

    /** S3A path to the Parquet partition for {@link #batchDate}. */
    public String parquetInputPath() {
        return String.format("s3a://%s/vessel-events/date=%s/", s3Bucket, batchDate);
    }

    private static String env(String key, String defaultValue) {
        String v = System.getenv(key);
        return (v != null && !v.isBlank()) ? v : defaultValue;
    }
}
