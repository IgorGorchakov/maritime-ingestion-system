package com.maritime.spark.jobs;

import com.maritime.spark.SparkJobProperties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Properties;

/**
 * Shared JDBC write helper injected into every Spark batch job.
 *
 * <p>Centralises the {@code df.write().jdbc()} call that was previously
 * duplicated (with slight variations) across all three jobs. Each job
 * receives this by constructor injection, making the JDBC dependency
 * explicit and the write behaviour consistent.
 *
 * <h3>Why Spark's df.write().jdbc() and not JdbcTemplate?</h3>
 * Spark's JDBC sink runs inside the executor (the driver in local mode),
 * handles schema creation automatically, and can write in parallel partitions.
 * {@code JdbcTemplate} is a single-threaded driver-side API suited to the
 * service modules — not to Spark's distributed write path.
 *
 * <h3>Credentials source</h3>
 * JDBC URL and credentials come from {@link SparkJobProperties} — the single
 * source of truth injected at construction time. {@code jdbcProps} is built
 * once in the constructor and reused across all {@link #write} calls.
 */
@Component
public class JobWriter {

    private static final Logger log = LoggerFactory.getLogger(JobWriter.class);

    private final String     jdbcUrl;
    private final Properties jdbcProps;

    public JobWriter(SparkJobProperties props) {
        this.jdbcUrl = props.getDbUrl();

        // Built once — user/password/driver are constants for the job lifetime.
        this.jdbcProps = new Properties();
        this.jdbcProps.setProperty("user",     props.getDbUser());
        this.jdbcProps.setProperty("password", props.getDbPass());
        this.jdbcProps.setProperty("driver",   "org.postgresql.Driver");
    }

    /**
     * Write {@code df} to {@code table} using {@link SaveMode#Overwrite} with
     * {@code truncate=true}: the table schema (indexes, grants) is preserved
     * while its rows are replaced. Re-running for the same date is idempotent.
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
