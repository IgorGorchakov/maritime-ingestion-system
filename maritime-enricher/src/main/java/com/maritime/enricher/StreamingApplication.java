package com.maritime.enricher;

import com.maritime.enricher.config.properties.EnricherKafkaProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Entry point for the Maritime Enricher Service (port 8082).
 *
 * Stateless ETL only: validate → dedup → PostGIS zone enrichment →
 * risk score → publish to {@code maritime.enriched}.
 *
 * Stateful behavioural detection (Kafka Streams / RocksDB) runs in the
 * separate {@code maritime-detection} module (port 8086).
 */
@SpringBootApplication
@EnableConfigurationProperties(EnricherKafkaProperties.class)
public class StreamingApplication {
    public static void main(String[] args) {
        SpringApplication.run(StreamingApplication.class, args);
    }
}
