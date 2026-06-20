package com.maritime.spark;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Typed configuration for all Spark batch jobs, bound from
 * {@code application.properties} or environment variables via Spring's
 * {@code @Value} and relaxed binding:
 * {@code SPARK_JOB_COLD_TIER_DIR} → {@code spark.job.cold-tier-dir}, etc.
 *
 * <h3>Validation</h3>
 * {@link #validate()} is annotated {@code @PostConstruct} so Spring calls it
 * immediately after construction, before any job bean receives this as a
 * dependency. A missing or blank required property fails fast with a clear
 * {@link IllegalStateException} rather than a confusing {@link NullPointerException}
 * deep inside Spark's JDBC writer.
 *
 * <p>Note: {@code @NotBlank} annotations from JSR-303 are not used here because
 * {@link org.springframework.context.annotation.AnnotationConfigApplicationContext}
 * does not automatically wire a {@code MethodValidationPostProcessor} — the
 * annotations would be silently ignored. The explicit {@code @PostConstruct}
 * guard is simpler, always runs, and produces a readable error message.
 *
 * <h3>Property table</h3>
 * <pre>
 * Property                    Env var                   Default
 * ─────────────────────────────────────────────────────────────────────────────
 * spark.job.cold-tier-dir     SPARK_JOB_COLD_TIER_DIR   ./data/cold
 * spark.job.db-url            SPARK_JOB_DB_URL           jdbc:postgresql://localhost:5432/maritime
 * spark.job.db-user           SPARK_JOB_DB_USER          postgres
 * spark.job.db-pass           SPARK_JOB_DB_PASS          postgres
 * spark.job.batch-date        SPARK_JOB_BATCH_DATE       yesterday (ISO-8601)
 * </pre>
 */
@Component
public class SparkJobProperties {

    private static final Logger log = LoggerFactory.getLogger(SparkJobProperties.class);

    private final String coldTierDir;
    private final String dbUrl;
    private final String dbUser;
    private final String dbPass;   // intentionally no blank-check — empty password is valid
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

    /**
     * Validate required properties immediately after Spring constructs this bean.
     * Fails fast with a descriptive message rather than propagating a null value
     * into Spark's execution engine.
     */
    @PostConstruct
    void validate() {
        List<String> errors = new ArrayList<>();
        if (isBlank(coldTierDir)) errors.add("spark.job.cold-tier-dir must not be blank");
        if (isBlank(dbUrl))       errors.add("spark.job.db-url must not be blank");
        if (isBlank(dbUser))      errors.add("spark.job.db-user must not be blank");
        if (isBlank(batchDate))   errors.add("spark.job.batch-date must not be blank");

        if (!errors.isEmpty()) {
            throw new IllegalStateException(
                    "SparkJobProperties validation failed:\n  - " + String.join("\n  - ", errors));
        }
        log.info("SparkJobProperties validated — date={} coldTierDir={} dbUrl={}",
                batchDate, coldTierDir, dbUrl);
    }

    public String getColdTierDir() { return coldTierDir; }
    public String getDbUrl()       { return dbUrl; }
    public String getDbUser()      { return dbUser; }
    public String getDbPass()      { return dbPass; }
    public String getBatchDate()   { return batchDate; }

    /**
     * Absolute {@code file://} URI for the JSON partition matching
     * {@link #getBatchDate()}. Spark's LocalFileSystem requires an absolute path.
     */
    public String inputPath() {
        String absBase = Path.of(coldTierDir).toAbsolutePath().normalize().toString();
        return String.format("file://%s/vessel-events/date=%s/", absBase, batchDate);
    }

    /** @deprecated Use {@link #inputPath()} */
    @Deprecated
    public String parquetInputPath() {
        return inputPath();
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
