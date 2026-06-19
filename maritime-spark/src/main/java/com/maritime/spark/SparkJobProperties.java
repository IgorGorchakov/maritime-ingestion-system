package com.maritime.spark;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.validation.constraints.NotBlank;
import java.nio.file.Path;

/**
 * Typed configuration for all Spark batch jobs, bound by Spring's {@code @Value}
 * from {@code application.properties} or environment variables.
 *
 * <p>Spring's relaxed binding maps environment variables to properties:
 * {@code SPARK_JOB_COLD_TIER_DIR} → {@code spark.job.cold-tier-dir}, etc.
 * This means the same class works for both local development
 * ({@code application.properties}) and CI/production (env vars via
 * {@code spark-submit --conf spark.hadoop.XXX=...}).
 *
 * <p>Replaces the previous {@code JobConfig} static factory which read
 * {@code System.getenv()} directly — that approach gave no validation, no IDE
 * completion, and required reflection in tests to bypass the private constructor.
 *
 * <h3>Property table</h3>
 * <pre>
 * Property                    Env var equivalent        Default
 * ─────────────────────────────────────────────────────────────────────────────
 * spark.job.cold-tier-dir     SPARK_JOB_COLD_TIER_DIR   ./data/cold
 * spark.job.db-url            SPARK_JOB_DB_URL           jdbc:postgresql://localhost:5432/maritime
 * spark.job.db-user           SPARK_JOB_DB_USER          postgres
 * spark.job.db-pass           SPARK_JOB_DB_PASS          postgres
 * spark.job.batch-date        SPARK_JOB_BATCH_DATE       yesterday (ISO date)
 * </pre>
 */
@Component
public class SparkJobProperties {

    @NotBlank
    private final String coldTierDir;

    @NotBlank
    private final String dbUrl;

    @NotBlank
    private final String dbUser;

    // intentionally no @NotBlank — empty password is valid for local Postgres
    private final String dbPass;

    @NotBlank
    private final String batchDate;

    public SparkJobProperties(
            @Value("${spark.job.cold-tier-dir:./data/cold}")
            String coldTierDir,

            @Value("${spark.job.db-url:jdbc:postgresql://localhost:5432/maritime}")
            String dbUrl,

            @Value("${spark.job.db-user:postgres}")
            String dbUser,

            @Value("${spark.job.db-pass:postgres}")
            String dbPass,

            @Value("${spark.job.batch-date:#{T(java.time.LocalDate).now().minusDays(1).toString()}}")
            String batchDate) {

        this.coldTierDir = coldTierDir;
        this.dbUrl       = dbUrl;
        this.dbUser      = dbUser;
        this.dbPass      = dbPass;
        this.batchDate   = batchDate;
    }

    public String getColdTierDir() { return coldTierDir; }
    public String getDbUrl()       { return dbUrl; }
    public String getDbUser()      { return dbUser; }
    public String getDbPass()      { return dbPass; }
    public String getBatchDate()   { return batchDate; }

    /**
     * {@code file://} URI pointing at the Parquet partition for {@link #getBatchDate()}.
     *
     * <p>Resolved to an absolute path so Spark's {@code LocalFileSystem} URI is
     * valid regardless of the job's working directory. The {@code date=} segment
     * is the Hive-style partition written by {@code FileSystemParquetColdTier};
     * partition discovery exposes it as a DataFrame column so Spark can prune all
     * other date partitions when filtering on {@code date = batchDate}.
     */
    public String parquetInputPath() {
        String absBase = Path.of(coldTierDir).toAbsolutePath().normalize().toString();
        return String.format("file://%s/vessel-events/date=%s/", absBase, batchDate);
    }
}
