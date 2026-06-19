package com.maritime.spark.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.maritime.spark.SparkJobProperties;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * Spring {@code @Configuration} that wires the two infrastructure beans shared
 * across all Spark batch jobs: the {@link SparkSession} and the {@link DataSource}.
 *
 * <h3>SparkSession lifecycle</h3>
 * {@code destroyMethod = "stop"} on the {@link #sparkSession} bean means Spring
 * calls {@code SparkSession.stop()} when the application context closes —
 * equivalent to the explicit {@code spark.stop()} calls that used to live in
 * each job's {@code main()}. This centralises teardown and ensures the session
 * is stopped exactly once even when multiple jobs run in the same JVM.
 *
 * <h3>DataSource</h3>
 * HikariCP supplies a connection pool to the JDBC write path inside each job.
 * Jobs receive it by constructor injection and pass it to
 * {@link com.maritime.spark.jobs.JobWriter#write}, replacing the manual
 * {@code java.util.Properties} block that previously appeared in each job.
 *
 * <h3>Why not Spring Boot DataSourceAutoConfiguration?</h3>
 * This module deliberately excludes Spring Boot to avoid its managed
 * Jackson/Avro/Netty versions conflicting with Spark's own bundled copies.
 * HikariCP is configured manually here instead of relying on autoconfiguration.
 */
@Configuration
public class SparkConfig {

    @Bean(destroyMethod = "stop")
    public SparkSession sparkSession(
            @Value("${spring.application.name:maritime-spark}") String appName,
            @Value("${spark.master:local[*]}") String master) {

        return SparkSession.builder()
                .appName(appName)
                .master(master)
                // Use the Avro-aware Parquet reader so EnrichedVesselEvent field
                // names drive column mapping when reading the cold tier.
                .config("spark.sql.parquet.enableVectorizedReader", "false")
                // Adaptive query planning — on for production.
                .config("spark.sql.adaptive.enabled", "true")
                // No HTTP server needed for batch jobs.
                .config("spark.ui.enabled", "false")
                .getOrCreate();
    }

    @Bean(destroyMethod = "close")
    public DataSource dataSource(SparkJobProperties props) {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(props.getDbUrl());
        cfg.setUsername(props.getDbUser());
        cfg.setPassword(props.getDbPass());
        cfg.setDriverClassName("org.postgresql.Driver");
        // Batch jobs write serially; a pool of 2 covers the brief overlap between
        // an active write and the subsequent count() call.
        cfg.setMaximumPoolSize(2);
        cfg.setPoolName("spark-jdbc-pool");
        return new HikariDataSource(cfg);
    }
}
