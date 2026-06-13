package com.maritime.common.observability;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;

/**
 * Ships the cross-cutting correlation-id beans from this library module so every
 * service picks them up with zero wiring.
 *
 * <p>{@code maritime-common} is not component-scanned by the services (their scan roots
 * are their own {@code com.maritime.<service>} packages), so a plain {@code @Component}
 * here would be invisible. Registering this class in
 * {@code META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports}
 * is the idiomatic Spring Boot 3 way to contribute beans from a dependency.
 *
 * <p>Only the HTTP filter and the Kafka record interceptor are beans. The Kafka
 * <em>producer</em> interceptor is instantiated by the Kafka client itself (named via
 * {@code ProducerConfig.INTERCEPTOR_CLASSES_CONFIG}), so it is intentionally not a bean.
 */
@AutoConfiguration
public class ObservabilityAutoConfiguration {

    @Bean
    public CorrelationIdHttpFilter correlationIdHttpFilter() {
        return new CorrelationIdHttpFilter();
    }

    @Bean
    public CorrelationIdRecordInterceptor correlationIdRecordInterceptor() {
        return new CorrelationIdRecordInterceptor();
    }
}
