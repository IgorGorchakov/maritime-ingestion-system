package com.maritime.ingestion.config.properties;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Typed Kafka configuration for the {@code maritime-ingestion} service, bound from
 * {@code application.properties} under the {@code maritime.ingestion.kafka} prefix.
 *
 * <p>Replaces the two {@code @Value} fields in
 * {@link com.maritime.ingestion.config.KafkaProducerConfig} (audit H4). Defaults live
 * only in {@code application.properties}; there are no inline Java defaults.
 *
 * <h3>Property table</h3>
 * <pre>
 * Property                                    Env var                  Default
 * ───────────────────────────────────────────────────────────────────────────────
 * maritime.ingestion.kafka.bootstrap-servers  KAFKA_BOOTSTRAP_SERVERS  localhost:9092
 * maritime.ingestion.kafka.schema-registry-url SCHEMA_REGISTRY_URL     http://localhost:8085
 * </pre>
 */
@ConfigurationProperties(prefix = "maritime.ingestion.kafka")
public record IngestionKafkaProperties(
        @NotBlank String bootstrapServers,
        @NotBlank String schemaRegistryUrl
) {}
