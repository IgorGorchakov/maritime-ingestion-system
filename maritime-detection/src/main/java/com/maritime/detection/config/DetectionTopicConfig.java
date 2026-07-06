package com.maritime.detection.config;

import com.maritime.common.kafka.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import com.maritime.detection.config.DetectionProperties;
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

    private final DetectionProperties config;

    public DetectionTopicConfig(DetectionProperties config) {
        this.config = config;
    }

    @Bean
    public KafkaAdmin kafkaAdmin() {
        return new KafkaAdmin(Map.of("bootstrap.servers", config.bootstrapServers()));
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

    /**
     * Output topic for raw H3 cell crossing events.
     * Produced by HexCrossingProcessor; consumed by HexCrossingEnricherService.
     */
    @Bean
    public NewTopic maritimeHexCrossings() {
        return TopicBuilder.name(Topics.HEX_CROSSINGS).partitions(PARTITIONS).replicas(-1).build();
    }

    /**
     * Dead-letter topic for unprocessable raw crossing events.
     */
    @Bean
    public NewTopic maritimeHexCrossingsDlt() {
        return TopicBuilder.name(Topics.HEX_CROSSINGS_DLT).partitions(PARTITIONS).replicas(-1).build();
    }

    /**
     * Internal changelog topic for the hex-cell-store RocksDB store.
     * Declared explicitly to guarantee the correct partition count.
     */
    @Bean
    public NewTopic hexCellStoreChangelog() {
        return TopicBuilder.name("maritime-detection-topology-hex-cell-store-changelog")
                .partitions(PARTITIONS).replicas(-1).build();
    }
}
