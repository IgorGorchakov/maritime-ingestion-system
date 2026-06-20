package com.maritime.spark.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * Root Spring configuration for the maritime-spark module.
 *
 * <p>Drives the component scan and property source for the
 * {@link org.springframework.context.annotation.AnnotationConfigApplicationContext}
 * started by {@link com.maritime.spark.SparkApplication}. Keeping this class
 * separate from the entry-point means {@code SparkApplication} is a plain class
 * with only a {@code main()} method — it is not itself a bean in the context it
 * creates, which avoids the antipattern of an entry-point class registering itself
 * as a {@code @Configuration} bean.
 *
 * <p>Component scan covers the full {@code com.maritime.spark} tree, picking up:
 * <ul>
 *   <li>{@link SparkConfig} — {@link org.apache.spark.sql.SparkSession} bean</li>
 *   <li>{@link com.maritime.spark.SparkJobProperties} — typed job configuration</li>
 *   <li>{@link com.maritime.spark.jobs.JobWriter} — shared JDBC write helper</li>
 *   <li>{@link com.maritime.spark.jobs.DailyVesselAggregatesJob},
 *       {@link com.maritime.spark.jobs.RiskRollupJob},
 *       {@link com.maritime.spark.jobs.LoiteringHotspotJob} — the three batch jobs</li>
 * </ul>
 */
@Configuration
@ComponentScan("com.maritime.spark")
@PropertySource("classpath:application.properties")
public class SparkContextConfig {
}
