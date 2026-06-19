package com.maritime.spark.jobs;

import com.maritime.spark.SparkJobProperties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.Properties;

/**
 * Shared JDBC write helper, injected into every Spark batch job.
 *
 * <p>Previously each job called a static {@code writeToPostgres} method,
 * coupling unrelated classes and making the JDBC dependency invisible in each
 * job's constructor. Extracting this into a Spring-managed {@code @Component}
 * makes the dependency explicit, injectable, and replaceable in tests without
 * static mocking.
 *
 * <h3>Why Spark's df.write().jdbc() and not JdbcTemplate?</h3>
 * Spark's JDBC sink runs inside the executor (or the driver in local mode),
 * writes in parallel partitions, and handles schema creation automatically.
 * {@code JdbcTemplate} is a single-threaded driver-side API — appropriate for
 * the service modules but not for Spark's distributed write path.
 *
 * <h3>DataSource role</h3>
 * The injected {@link DataSource} is used only for health-checking the
 * connection at startup via {@link SparkConfig}. The actual JDBC URL and
 * credentials passed to {@code df.write().jdbc()} come from
 * {@link SparkJobProperties} — the single source of truth — so there is no
 * risk of the URL drifting between the pool and the Spark write path.
 */
@Component
public class JobWriter {

    private static final Logger log = LoggerFactory.getLogger(JobWriter.class);

    private final String     jdbcUrl;
    private final Properties jdbcProps;  // built once; user/password/driver never change

    public JobWriter(SparkJobProperties props, DataSource dataSource) {
        this.jdbcUrl = props.getDbUrl();

        // Build the Properties object once in the constructor. Every write()
        // call reuses the same instance — no repeated string allocations.
        this.jdbcProps = new Properties();
        this.jdbcProps.setProperty("user",     props.getDbUser());
        this.jdbcProps.setProperty("password", props.getDbPass());
        this.jdbcProps.setProperty("driver",   "org.postgresql.Driver");
    }

    /**
     * Write {@code df} to {@code table} using {@link SaveMode#Overwrite} with
     * {@code truncate=true}, so the table is preserved (indexes, grants) while
     * its rows are replaced. Re-running the job for the same date is idempotent.
     *
     * @param df    aggregated DataFrame to persist
     * @param table target table name; auto-created by Spark on first run
     */
    public void write(Dataset<Row> df, String table) {
        log.info("Writing to table={} url={}", table, jdbcUrl);
        df.write()
                .mode(SaveMode.Overwrite)
                .option("truncate", "true")
                .jdbc(jdbcUrl, table, jdbcProps);
    }
}
