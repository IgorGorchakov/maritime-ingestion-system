package com.maritime.enricher.config;

import com.maritime.common.kafka.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Platform-wide Kafka topic registry for topics owned by the enricher service.
 *
 * Topic declarations are idempotent — KafkaAdmin issues CreateTopics, which is a
 * no-op if the topic already exists. The enricher starts first (port 8082) so
 * topics it declares are available before downstream services connect.
 *
 * Topics owned by other services:
 *   maritime.detections, detection changelog → maritime-detection (port 8086)
 */
@Configuration
public class TopicConfig {

    private static final int PARTITIONS = 3;

    @Bean
    public NewTopic maritimeAisRaw() {
        return TopicBuilder.name(Topics.AIS_RAW).partitions(PARTITIONS).replicas(-1).build();
    }

    @Bean
    public NewTopic maritimeEnriched() {
        return TopicBuilder.name(Topics.ENRICHED).partitions(PARTITIONS).replicas(-1).build();
    }

    @Bean
    public NewTopic maritimeAisRawDlt() {
        return TopicBuilder.name(Topics.AIS_RAW_DLT).partitions(PARTITIONS).replicas(-1).build();
    }

    @Bean
    public NewTopic maritimeEnrichedDlt() {
        return TopicBuilder.name(Topics.ENRICHED_DLT).partitions(PARTITIONS).replicas(-1).build();
    }

    @Bean
    public NewTopic maritimeAisQuarantine() {
        return TopicBuilder.name(Topics.QUARANTINE).partitions(PARTITIONS).replicas(-1).build();
    }
}
