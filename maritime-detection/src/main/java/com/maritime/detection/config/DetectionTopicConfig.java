package com.maritime.detection.config;

import com.maritime.common.kafka.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.config.TopicBuilder;

import java.util.Map;

/**
 * Declares the Kafka topics owned by the detection service.
 *
 * Topic declarations are idempotent — {@link KafkaAdmin} issues a
 * {@code CreateTopics} request that is a no-op if the topic already exists.
 * The enricher service declares the platform's input topics; this config
 * only adds the two topics the detection topology produces or relies on.
 */
@Configuration
public class DetectionTopicConfig {

    private static final int PARTITIONS = 3;

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        return new KafkaAdmin(Map.of("bootstrap.servers", bootstrapServers));
    }

    /**
     * Output topic for flagged detection events (loitering, dark vessel, speed anomaly).
     * Separate from {@code maritime.enriched} to keep the data flow a strict DAG.
     */
    @Bean
    public NewTopic maritimeDetections() {
        return TopicBuilder.name(Topics.DETECTIONS).partitions(PARTITIONS).replicas(-1).build();
    }

    /**
     * Internal changelog topic for the {@code vessel-state-store} RocksDB store.
     * Declared explicitly to guarantee the correct partition count; Kafka Streams
     * would auto-create it, but explicit declaration wins on brokers where
     * auto-topic-creation is disabled.
     */
    @Bean
    public NewTopic vesselStateStoreChangelog() {
        return TopicBuilder.name("maritime-detection-topology-vessel-state-store-changelog")
                .partitions(PARTITIONS).replicas(-1).build();
    }
}
