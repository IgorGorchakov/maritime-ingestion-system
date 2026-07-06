package com.maritime.detection.config;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Typed configuration for the {@code maritime-detection} service, bound from
 * {@code application.properties} (or environment-variable overrides) under the
 * {@code maritime.detection} prefix.
 *
 * <p>This record is the single source of truth for every tunable value in this
 * module. Defaults live only in {@code application.properties}; the {@code @Value}
 * inline defaults that previously scattered them across {@link MaritimeTopology} and
 * {@link DetectionTopicConfig} have been removed (audit H4).
 *
 * <p>{@code @NotBlank} is enforced at startup by
 * {@code @EnableConfigurationProperties} + the {@code spring-boot-starter-validation}
 * dependency — a missing or blank property fails the application context refresh with
 * a clear {@link org.springframework.boot.context.properties.bind.BindException}
 * rather than a {@link NullPointerException} deep in Kafka Streams.
 *
 * <h3>Property table</h3>
 * <pre>
 * Property                              Env var                      Default (in application.properties)
 * ──────────────────────────────────────────────────────────────────────────────────────────────────────
 * maritime.detection.bootstrap-servers  KAFKA_BOOTSTRAP_SERVERS      localhost:9092
 * maritime.detection.schema-registry-url SCHEMA_REGISTRY_URL         http://localhost:8085
 * maritime.detection.state-dir          KAFKA_STREAMS_STATE_DIR      /tmp/kafka-streams/maritime
 * </pre>
 */
@ConfigurationProperties(prefix = "maritime.detection")
public record DetectionProperties(
        @NotBlank String bootstrapServers,
        @NotBlank String schemaRegistryUrl,
        @NotBlank String stateDir
) {}
