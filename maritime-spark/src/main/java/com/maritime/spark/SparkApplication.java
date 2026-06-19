package com.maritime.spark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * Entry point for the Maritime Spark batch layer.
 *
 * <h3>Why AnnotationConfigApplicationContext and not SpringApplication?</h3>
 * {@code SpringApplication} bootstraps Spring Boot's full autoconfiguration
 * machinery, which pulls in Spring Boot's managed versions of Jackson, Avro, and
 * Netty — all version-conflicting with Spark 3.x's own bundled copies. Using
 * {@link AnnotationConfigApplicationContext} directly starts only the Spring IoC
 * container (DI, {@code @Value}, {@code @Scheduled}, {@link org.springframework.boot.ApplicationRunner})
 * with zero autoconfiguration, keeping Spark's classpath isolated.
 *
 * <h3>Job execution</h3>
 * Each Spark job is a Spring {@link org.springframework.stereotype.Component}
 * implementing {@link org.springframework.boot.ApplicationRunner}. After the
 * context is refreshed, this class iterates the {@code ApplicationRunner} beans
 * in {@link org.springframework.core.annotation.Order} order and invokes them,
 * replicating the behaviour Spring Boot provides without its autoconfiguration.
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
@Configuration
@ComponentScan("com.maritime.spark")
@PropertySource("classpath:application.properties")
public class SparkApplication {

    private static final Logger log = LoggerFactory.getLogger(SparkApplication.class);

    public static void main(String[] args) {
        log.info("Maritime Spark Application starting");

        try (AnnotationConfigApplicationContext ctx =
                     new AnnotationConfigApplicationContext(SparkApplication.class)) {

            ApplicationArguments appArgs = new DefaultApplicationArguments(args);

            // Run all ApplicationRunner beans in @Order sequence.
            // This mirrors what SpringApplication does without loading autoconfiguration.
            ctx.getBeansOfType(org.springframework.boot.ApplicationRunner.class)
               .entrySet()
               .stream()
               .sorted(java.util.Comparator.comparingInt(e ->
                       org.springframework.core.annotation.AnnotationUtils
                               .findAnnotation(e.getValue().getClass(),
                                       org.springframework.core.annotation.Order.class) != null
                               ? org.springframework.core.annotation.AnnotationUtils
                                       .findAnnotation(e.getValue().getClass(),
                                               org.springframework.core.annotation.Order.class).value()
                               : Integer.MAX_VALUE))
               .forEach(e -> {
                   try {
                       log.info("Running job: {}", e.getKey());
                       e.getValue().run(appArgs);
                   } catch (Exception ex) {
                       log.error("Job {} failed", e.getKey(), ex);
                       throw new RuntimeException("Job execution failed: " + e.getKey(), ex);
                   }
               });
        }

        log.info("Maritime Spark Application finished");
    }
}
