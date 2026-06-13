package com.maritime.streaming.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class TopicConfig {

    private static final int PARTITIONS = 3;

    @Bean
    public NewTopic maritimeAisRaw() {
        return TopicBuilder.name("maritime.ais.raw").partitions(PARTITIONS).replicas(-1).build();
    }

    @Bean
    public NewTopic maritimeEnriched() {
        return TopicBuilder.name("maritime.enriched").partitions(PARTITIONS).replicas(-1).build();
    }

    // DLT topics — created automatically when DeadLetterPublishingRecoverer writes to them,
    // but declaring them explicitly ensures correct partition count from the start.
    @Bean
    public NewTopic maritimeAisRawDlt() {
        return TopicBuilder.name("maritime.ais.raw.DLT").partitions(PARTITIONS).replicas(-1).build();
    }

    @Bean
    public NewTopic maritimeEnrichedDlt() {
        return TopicBuilder.name("maritime.enriched.DLT").partitions(PARTITIONS).replicas(-1).build();
    }

    // Quarantine topic for invalid vessel events (Phase 3: E→V→T→L)
    @Bean
    public NewTopic maritimeAisQuarantine() {
        return TopicBuilder.name("maritime.ais.quarantine").partitions(PARTITIONS).replicas(-1).build();
    }

    // Phase 6: Kafka Streams changelog topic for the vessel-state-store.
    // Declared explicitly to guarantee correct partition count and replication.
    // The topology also auto-creates it, but explicit declaration wins.
    @Bean
    public NewTopic maritimeStateChangelog() {
        return TopicBuilder.name("maritime-detection-topology-vessel-state-store-changelog")
                .partitions(PARTITIONS).replicas(-1).build();
    }

    /**
     * Output topic for the detection topology (loitering, dark vessel, speed anomaly).
     *
     * Kept separate from maritime.enriched so the topology's output never feeds
     * back into its own input — the data flow is a strict DAG. The storage service
     * subscribes to both topics in independent @KafkaListener methods.
     */
    @Bean
    public NewTopic maritimeDetections() {
        return TopicBuilder.name("maritime.detections").partitions(PARTITIONS).replicas(-1).build();
    }
}
