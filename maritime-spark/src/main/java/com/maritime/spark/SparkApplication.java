package com.maritime.spark;

import com.maritime.spark.config.SparkContextConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.annotation.AnnotationAwareOrderComparator;

import java.util.List;

/**
 * Entry point for the Maritime Spark batch layer.
 *
 * <p>This is a plain class with a single {@code main()} method — it carries no
 * Spring annotations and is not a bean in the context it creates. The context
 * root is {@link SparkContextConfig}, which owns {@code @ComponentScan} and
 * {@code @PropertySource}. Separating the two prevents the entry-point class
 * from being registered as a {@code @Configuration} bean inside its own context.
 *
 * <h3>Why AnnotationConfigApplicationContext and not SpringApplication?</h3>
 * {@code SpringApplication} bootstraps Spring Boot's full autoconfiguration
 * machinery, pulling in Boot-managed versions of Jackson, Avro, and Netty —
 * all version-conflicting with Spark 3.x's own bundled copies.
 * {@link AnnotationConfigApplicationContext} starts only the Spring IoC container
 * with zero autoconfiguration, keeping Spark's classpath isolated.
 *
 * <h3>Job execution order</h3>
 * {@link ApplicationRunner} beans are sorted by
 * {@link AnnotationAwareOrderComparator} — the same comparator
 * {@code SpringApplication} uses internally — which respects
 * {@link org.springframework.core.annotation.Order @Order},
 * {@link org.springframework.core.Ordered}, and {@link org.springframework.core.PriorityOrdered}:
 * <ol>
 *   <li>{@code DailyVesselAggregatesJob} — {@code @Order(1)}</li>
 *   <li>{@code RiskRollupJob}            — {@code @Order(2)}</li>
 *   <li>{@code LoiteringHotspotJob}      — {@code @Order(3)}</li>
 * </ol>
 *
 * <h3>Running via spark-submit</h3>
 * <pre>{@code
 * spark-submit \
 *   --class com.maritime.spark.SparkApplication \
 *   --master yarn \
 *   target/maritime-spark-1.0.0-SNAPSHOT-shaded.jar
 * }</pre>
 *
 * <h3>Running locally</h3>
 * <pre>{@code
 * mvn exec:java -Plocal -Dexec.mainClass=com.maritime.spark.SparkApplication
 * }</pre>
 */
public class SparkApplication {

    private static final Logger log = LoggerFactory.getLogger(SparkApplication.class);

    public static void main(String[] args) {
        log.info("Maritime Spark Application starting");

        try (AnnotationConfigApplicationContext ctx =
                     new AnnotationConfigApplicationContext(SparkContextConfig.class)) {

            ApplicationArguments appArgs = new DefaultApplicationArguments(args);

            List<ApplicationRunner> runners =
                    ctx.getBeansOfType(ApplicationRunner.class)
                       .values()
                       .stream()
                       .sorted(AnnotationAwareOrderComparator.INSTANCE)
                       .toList();

            for (ApplicationRunner runner : runners) {
                String name = runner.getClass().getSimpleName();
                try {
                    log.info("Running job: {}", name);
                    runner.run(appArgs);
                    log.info("Job complete: {}", name);
                } catch (Exception ex) {
                    log.error("Job failed: {}", name, ex);
                    throw new RuntimeException("Job execution failed: " + name, ex);
                }
            }
        }

        log.info("Maritime Spark Application finished");
    }
}
