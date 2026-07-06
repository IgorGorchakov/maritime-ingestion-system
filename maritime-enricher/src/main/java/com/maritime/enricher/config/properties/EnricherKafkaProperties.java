package com.maritime.enricher.config.properties;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Typed Kafka configuration for the {@code maritime-enricher} service, bound from
 * {@code application.properties} under the {@code maritime.enricher.kafka} prefix.
 *
 * <p>Replaces the three scattered {@code @Value} fields that were previously in
 * {@link com.maritime.enricher.config.KafkaConsumerConfig} (audit H4). Defaults
 * live only in {@code application.properties}; there are no inline Java defaults.
 *
 * <p>{@code @NotBlank} causes a {@link org.springframework.boot.context.properties.bind.BindException}
 * at startup if any required property is absent or blank — faster and clearer than
 * a {@link NullPointerException} deep inside Kafka consumer setup.
 *
 * <h3>Contradiction resolved</h3>
 * The previous {@code @Value} default for {@code groupId} was {@code "enricher-service"},
 * while {@code application.properties} declared
 * {@code spring.kafka.consumer.group-id=streaming-service}. That silent disagreement
 * is fixed here: {@code maritime.enricher.kafka.group-id} is set to
 * {@code streaming-service} in {@code application.properties}, which is now the single
 * authoritative value.
 *
 * <h3>Property table</h3>
 * <pre>
 * Property                                  Env var                  Default
 * ─────────────────────────────────────────────────────────────────────────────────
 * maritime.enricher.kafka.bootstrap-servers  KAFKA_BOOTSTRAP_SERVERS  localhost:9092
 * maritime.enricher.kafka.schema-registry-url SCHEMA_REGISTRY_URL     http://localhost:8085
 * maritime.enricher.kafka.group-id           (none; set explicitly)   streaming-service
 * </pre>
 */
@ConfigurationProperties(prefix = "maritime.enricher.kafka")
public record EnricherKafkaProperties(
        @NotBlank String bootstrapServers,
        @NotBlank String schemaRegistryUrl,
        @NotBlank String groupId
) {}
