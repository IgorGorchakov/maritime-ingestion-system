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
 * Spring annotations and is never registered as a bean. The context root is
 * {@link SparkContextConfig}, a separate {@code @Configuration} class that owns
 * {@code @ComponentScan} and {@code @PropertySource}. This separation avoids the
 * antipattern of an entry-point class registering itself as a
 * {@code @Configuration} bean inside the context it creates.
 *
 * <h3>Why AnnotationConfigApplicationContext, not SpringApplication?</h3>
 * {@code SpringApplication} bootstraps Spring Boot's full autoconfiguration
 * machinery, which pulls in Boot-managed versions of Jackson, Avro, and Netty.
 * All three version-conflict with Spark 3.x's own bundled copies, causing
 * {@code ClassCastException} or {@code NoSuchMethodError} at runtime.
 * {@link AnnotationConfigApplicationContext} starts only the Spring IoC container
 * (DI, {@code @Value}, {@code @PostConstruct}, {@link ApplicationRunner}) with
 * zero autoconfiguration, keeping Spark's classpath isolated.
 *
 * <h3>Context lifecycle</h3>
 * The context is opened in a {@code try}-with-resources block. When the block
 * exits — whether normally or via exception — the context's {@code close()} is
 * called, which triggers all {@code @PreDestroy} / {@code destroyMethod} hooks in
 * dependency order. Concretely: {@code SparkSession.stop()} is called
 * automatically, even if a job throws. No explicit {@code spark.stop()} is needed
 * anywhere in job code.
 *
 * <h3>Startup validation</h3>
 * {@link SparkJobProperties#validate()} is annotated {@code @PostConstruct} and
 * runs immediately after the context is refreshed, before any job bean is
 * instantiated. A missing or blank required property (db URL, cold-tier path,
 * batch date) throws {@link IllegalStateException} here with a clear message
 * rather than propagating a {@code NullPointerException} deep into Spark's
 * execution engine.
 *
 * <h3>Job execution order</h3>
 * All {@link ApplicationRunner} beans are collected from the context and sorted
 * by {@link AnnotationAwareOrderComparator#INSTANCE} — the same comparator
 * {@code SpringApplication} uses in {@code callRunners()}. It respects
 * {@link org.springframework.core.annotation.Order @Order},
 * {@link org.springframework.core.Ordered}, and
 * {@link org.springframework.core.PriorityOrdered}. The current jobs and their
 * declared order are listed in {@link SparkContextConfig}.
 *
 * <h3>Failure behaviour</h3>
 * If any job throws, the exception is logged with full context, wrapped in a
 * {@link RuntimeException}, and re-thrown. The {@code try}-with-resources then
 * closes the context (stopping Spark), and the JVM exits with a non-zero status.
 * Subsequent jobs in the sequence are not attempted — a failed batch run should
 * be investigated before the next job's output is allowed to overwrite partial
 * results.
 *
 * <h3>Running via spark-submit</h3>
 * <pre>{@code
 * spark-submit \
 *   --class com.maritime.spark.SparkApplication \
 *   --master yarn \
 *   --conf spark.job.batch-date=2024-03-15 \
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

        // try-with-resources guarantees context.close() on both normal exit and
        // exception, which triggers SparkSession.stop() via destroyMethod="stop"
        // on the SparkConfig bean — no manual spark.stop() needed in job code.
        try (AnnotationConfigApplicationContext ctx =
                     new AnnotationConfigApplicationContext(SparkContextConfig.class)) {

            // SparkJobProperties.validate() already ran as @PostConstruct during
            // context refresh above — if any required property was blank, we would
            // not reach this line.

            // Wrap raw String[] args so jobs can use the typed ApplicationArguments
            // API (getOptionValues, getNonOptionArgs) if they choose to.
            ApplicationArguments appArgs = new DefaultApplicationArguments(args);

            // Collect all ApplicationRunner beans and sort by @Order.
            // AnnotationAwareOrderComparator is the same comparator SpringApplication
            // uses in callRunners() — it correctly handles @Order, Ordered, and
            // PriorityOrdered, unlike a manual AnnotationUtils.findAnnotation sort.
            List<ApplicationRunner> runners =
                    ctx.getBeansOfType(ApplicationRunner.class)
                       .values()
                       .stream()
                       .sorted(AnnotationAwareOrderComparator.INSTANCE)
                       .toList();

            log.info("Found {} job(s) to run: {}",
                    runners.size(),
                    runners.stream()
                           .map(r -> r.getClass().getSimpleName())
                           .toList());

            for (ApplicationRunner runner : runners) {
                String name = runner.getClass().getSimpleName();
                try {
                    log.info("Starting job: {}", name);
                    runner.run(appArgs);
                    log.info("Finished job: {}", name);
                } catch (Exception ex) {
                    log.error("Job failed — stopping pipeline: {}", name, ex);
                    // Re-throw so the try-with-resources closes the context
                    // (stopping Spark) before the JVM exits non-zero.
                    throw new RuntimeException("Job execution failed: " + name, ex);
                }
            }
        }

        log.info("Maritime Spark Application finished");
    }
}
