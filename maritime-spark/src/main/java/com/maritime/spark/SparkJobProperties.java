package com.maritime.spark;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Typed configuration for all Spark batch jobs, bound from
 * {@code application.properties} or environment variables via Spring Boot's
 * {@code @ConfigurationProperties} relaxed binding:
 * {@code SPARK_JOB_COLD_TIER_DIR} → {@code spark.job.cold-tier-dir}, etc.
 *
 * <h3>Validation</h3>
 * {@code @NotBlank} fields are validated by the
 * {@link org.springframework.boot.context.properties.ConfigurationPropertiesBindingPostProcessor}
 * registered in {@link com.maritime.spark.config.SparkContextConfig}. A missing or blank
 * required property causes a
 * {@link org.springframework.boot.context.properties.bind.BindException} during
 * context refresh — before any job bean receives this as a dependency — with a clear
 * message rather than a {@link NullPointerException} deep in Spark's execution engine.
 *
 * <p>The previous {@code @Component} / {@code @Value} / {@code @PostConstruct} approach
 * is replaced here (audit H4). Defaults now live only in {@code application.properties};
 * there are no inline Java defaults.
 *
 * <h3>Property table</h3>
 * <pre>
 * Property                    Env var                   Default (in application.properties)
 * ─────────────────────────────────────────────────────────────────────────────────────────
 * spark.job.cold-tier-dir     SPARK_JOB_COLD_TIER_DIR   ./data/cold
 * spark.job.db-url            SPARK_JOB_DB_URL           jdbc:postgresql://localhost:5432/maritime
 * spark.job.db-user           SPARK_JOB_DB_USER          postgres
 * spark.job.db-pass           SPARK_JOB_DB_PASS          postgres
 * spark.job.batch-date        SPARK_JOB_BATCH_DATE       yesterday (SpEL, evaluated at bind time)
 * </pre>
 */
@ConfigurationProperties(prefix = "spark.job")
public record SparkJobProperties(
        @NotBlank String coldTierDir,
        @NotBlank String dbUrl,
        @NotBlank String dbUser,
                  String dbPass,     // nullable — empty password is valid
        @NotBlank String batchDate
) {
    /**
     * Absolute {@code file://} URI for the JSON partition matching {@link #batchDate()}.
     * Spark's LocalFileSystem requires an absolute path.
     */
    public String inputPath() {
        String absBase = java.nio.file.Path.of(coldTierDir).toAbsolutePath().normalize().toString();
        return String.format("file://%s/vessel-events/date=%s/", absBase, batchDate);
    }
}
