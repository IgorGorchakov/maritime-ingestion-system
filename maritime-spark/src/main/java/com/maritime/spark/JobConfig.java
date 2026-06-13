package com.maritime.spark;

/**
 * Runtime configuration for all Spark batch jobs.
 *
 * Values come from environment variables (production/CI) or fall back to
 * local-development defaults. Using env vars (rather than a properties file)
 * keeps the jobs compatible with both {@code spark-submit} and direct execution
 * without requiring a config file on the classpath.
 *
 * <pre>
 * Variable        Default                              Description
 * ───────────────────────────────────────────────────────────────────────────
 * COLD_TIER_DIR   ./data/cold                          Local Parquet cold tier
 *                                                        (written by maritime-storage)
 * DB_URL          jdbc:postgresql://localhost:5432/    Postgres JDBC URL
 *                   maritime
 * DB_USER         postgres
 * DB_PASS         postgres
 * BATCH_DATE      yesterday (ISO date)                 Date partition to process
 * </pre>
 */
public final class JobConfig {

    /** Filesystem base dir of the Parquet cold tier (shared with maritime-storage). */
    public final String coldTierDir;
    public final String dbUrl;
    public final String dbUser;
    public final String dbPass;
    /** ISO date (yyyy-MM-dd) of the partition to process. */
    public final String batchDate;

    private JobConfig(String coldTierDir, String dbUrl, String dbUser, String dbPass,
                      String batchDate) {
        this.coldTierDir = coldTierDir;
        this.dbUrl       = dbUrl;
        this.dbUser      = dbUser;
        this.dbPass      = dbPass;
        this.batchDate   = batchDate;
    }

    public static JobConfig fromEnv() {
        // Default batchDate = yesterday so a scheduled daily job always processes
        // the previous full day without requiring an explicit parameter.
        String yesterday = java.time.LocalDate.now()
                .minusDays(1)
                .toString();   // ISO-8601: yyyy-MM-dd

        return new JobConfig(
                env("COLD_TIER_DIR", "./data/cold"),
                env("DB_URL",  "jdbc:postgresql://localhost:5432/maritime"),
                env("DB_USER", "postgres"),
                env("DB_PASS", "postgres"),
                env("BATCH_DATE", yesterday)
        );
    }

    /**
     * {@code file://} path to the Parquet partition for {@link #batchDate}.
     *
     * <p>Resolved to an absolute path so Spark's {@code file://} URI is valid
     * regardless of the job's working directory. The {@code date=} segment is a
     * Hive-style partition the storage service writes, so partition discovery still
     * exposes it as a DataFrame column.
     */
    public String parquetInputPath() {
        String absBase = java.nio.file.Path.of(coldTierDir)
                .toAbsolutePath().normalize().toString();
        return String.format("file://%s/vessel-events/date=%s/", absBase, batchDate);
    }

    private static String env(String key, String defaultValue) {
        String v = System.getenv(key);
        return (v != null && !v.isBlank()) ? v : defaultValue;
    }
}
