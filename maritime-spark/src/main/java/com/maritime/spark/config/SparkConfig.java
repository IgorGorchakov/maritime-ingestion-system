package com.maritime.spark.config;

import com.maritime.spark.SparkJobProperties;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring {@code @Configuration} that owns the {@link SparkSession} bean.
 *
 * <h3>SparkSession lifecycle</h3>
 * {@code destroyMethod = "stop"} means Spring calls {@code SparkSession.stop()}
 * when the application context closes — ensuring teardown happens exactly once
 * regardless of how many jobs ran in the same JVM.
 *
 * <h3>No DataSource bean here</h3>
 * A HikariCP {@code DataSource} bean was previously declared here to satisfy
 * {@link com.maritime.spark.jobs.JobWriter}'s constructor. {@code JobWriter} no
 * longer uses a {@code DataSource} — JDBC credentials come directly from
 * {@link SparkJobProperties} and are passed to {@code df.write().jdbc()}.
 * HikariCP and its dependency are therefore removed entirely from this module.
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
                // Avro-aware Parquet reader: uses EnrichedVesselEvent field names
                // for column mapping when reading the cold tier.
                .config("spark.sql.parquet.enableVectorizedReader", "false")
                // Adaptive query planning — enabled for production.
                .config("spark.sql.adaptive.enabled", "true")
                // No Spark UI needed for batch jobs.
                .config("spark.ui.enabled", "false")
                .getOrCreate();
    }
}
