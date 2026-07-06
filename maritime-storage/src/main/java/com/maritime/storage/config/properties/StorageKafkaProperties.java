package com.maritime.storage.config.properties;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Typed Kafka configuration for the {@code maritime-storage} service, bound from
 * {@code application.properties} under the {@code maritime.storage.kafka} prefix.
 *
 * <p>Replaces the three scattered {@code @Value} fields in
 * {@link com.maritime.storage.config.KafkaConsumerConfig} (audit H4). Defaults
 * live only in {@code application.properties}; there are no inline Java defaults.
 *
 * <h3>Property table</h3>
 * <pre>
 * Property                                  Env var                  Default
 * ─────────────────────────────────────────────────────────────────────────────────
 * maritime.storage.kafka.bootstrap-servers  KAFKA_BOOTSTRAP_SERVERS  localhost:9092
 * maritime.storage.kafka.schema-registry-url SCHEMA_REGISTRY_URL     http://localhost:8085
 * maritime.storage.kafka.group-id           (none; set explicitly)   storage-service
 * </pre>
 */
@ConfigurationProperties(prefix = "maritime.storage.kafka")
public record StorageKafkaProperties(
        @NotBlank String bootstrapServers,
        @NotBlank String schemaRegistryUrl,
        @NotBlank String groupId
) {}
