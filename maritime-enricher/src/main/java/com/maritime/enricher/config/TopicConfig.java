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
    public NewTopic maritimeAisRawTerrestrial() {
        return TopicBuilder.name(Topics.AIS_RAW_TERRESTRIAL).partitions(PARTITIONS).replicas(-1).build();
    }

    @Bean
    public NewTopic maritimeAisRawSatellite() {
        return TopicBuilder.name(Topics.AIS_RAW_SATELLITE).partitions(PARTITIONS).replicas(-1).build();
    }

    @Bean
    public NewTopic maritimeAisRawVessel() {
        return TopicBuilder.name(Topics.AIS_RAW_VESSEL).partitions(PARTITIONS).replicas(-1).build();
    }

    @Bean
    public NewTopic maritimeEnriched() {
        return TopicBuilder.name(Topics.ENRICHED).partitions(PARTITIONS).replicas(-1).build();
    }

    @Bean
    public NewTopic maritimeAisRawTerrestrialDlt() {
        return TopicBuilder.name(Topics.AIS_RAW_TERRESTRIAL_DLT).partitions(PARTITIONS).replicas(-1).build();
    }

    @Bean
    public NewTopic maritimeAisRawSatelliteDlt() {
        return TopicBuilder.name(Topics.AIS_RAW_SATELLITE_DLT).partitions(PARTITIONS).replicas(-1).build();
    }

    @Bean
    public NewTopic maritimeAisRawVesselDlt() {
        return TopicBuilder.name(Topics.AIS_RAW_VESSEL_DLT).partitions(PARTITIONS).replicas(-1).build();
    }

    @Bean
    public NewTopic maritimeEnrichedDlt() {
        return TopicBuilder.name(Topics.ENRICHED_DLT).partitions(PARTITIONS).replicas(-1).build();
    }

    @Bean
    public NewTopic maritimeAisQuarantine() {
        return TopicBuilder.name(Topics.QUARANTINE).partitions(PARTITIONS).replicas(-1).build();
    }

    /**
     * Output topic for enriched H3 cell crossing events.
     * Produced by HexCrossingEnricherService with zone and risk context.
     */
    @Bean
    public NewTopic maritimeHexCrossingsEnriched() {
        return TopicBuilder.name(Topics.HEX_CROSSINGS_ENRICHED).partitions(PARTITIONS).replicas(-1).build();
    }

    /**
     * Dead-letter topic for unprocessable enriched crossing events.
     */
    @Bean
    public NewTopic maritimeHexCrossingsEnrichedDlt() {
        return TopicBuilder.name(Topics.HEX_CROSSINGS_ENRICHED + Topics.DLT_SUFFIX).partitions(PARTITIONS).replicas(-1).build();
    }
}
