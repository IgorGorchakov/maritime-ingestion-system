package com.maritime.spark.jobs;

import com.maritime.spark.SparkJobProperties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Shared JDBC write helper, injected into every Spark batch job.
 *
 * <p>Previously each job called the static {@code DailyVesselAggregatesJob.writeToPostgres}
 * method, coupling two unrelated classes and making the JDBC dependency invisible
 * in the job's constructor. Extracting this into a Spring-managed {@code @Component}
 * makes the dependency explicit, injectable, and replaceable in tests without
 * static mocking.
 *
 * <h3>Why Spark's df.write().jdbc() and not JdbcTemplate?</h3>
 * Spark's JDBC sink runs inside the executor (or the driver in local mode),
 * writes in parallel partitions, and handles schema creation automatically.
 * {@code JdbcTemplate} is a single-threaded driver-side API — appropriate for
 * the service modules but not for Spark's distributed write path.
 * The {@link DataSource} is used only to extract the JDBC URL and credentials;
 * Spark manages its own connections to the target database.
 */
@Component
public class JobWriter {

    private static final Logger log = LoggerFactory.getLogger(JobWriter.class);

    private final SparkJobProperties props;
    private final DataSource         dataSource;

    public JobWriter(SparkJobProperties props, DataSource dataSource) {
        this.props      = props;
        this.dataSource = dataSource;
    }

    /**
     * Write {@code df} to {@code table} in the configured Postgres instance,
     * overwriting any existing rows for the current batch date.
     *
     * <p>Uses {@link SaveMode#Overwrite} with {@code truncate=true} so the table
     * is retained (preserving indexes and grants) while its rows are replaced.
     * Re-running the job for the same date is therefore idempotent.
     *
     * @param df    the aggregated DataFrame to persist
     * @param table target Postgres table name (must already exist or be
     *              auto-created by Spark on first run)
     */
    public void write(Dataset<Row> df, String table) {
        String jdbcUrl = resolveUrl();

        Properties jdbcProps = new Properties();
        jdbcProps.setProperty("user",     props.getDbUser());
        jdbcProps.setProperty("password", props.getDbPass());
        jdbcProps.setProperty("driver",   "org.postgresql.Driver");

        log.info("Writing {} to table={} url={}", df.schema().simpleString(), table, jdbcUrl);

        df.write()
                .mode(SaveMode.Overwrite)
                .option("truncate", "true")
                .jdbc(jdbcUrl, table, jdbcProps);
    }

    /**
     * Extract the JDBC URL from the injected {@link DataSource} so a single
     * source of truth (the DataSource bean in {@link com.maritime.spark.config.SparkConfig})
     * owns the URL — jobs never reference {@link SparkJobProperties#getDbUrl()} directly.
     */
    private String resolveUrl() {
        try (var conn = dataSource.getConnection()) {
            return conn.getMetaData().getURL();
        } catch (SQLException e) {
            // Fall back to the property value if the DataSource metadata call fails
            // (e.g. in tests that substitute a thin stub).
            log.warn("Could not resolve JDBC URL from DataSource metadata, falling back to property", e);
            return props.getDbUrl();
        }
    }
}
