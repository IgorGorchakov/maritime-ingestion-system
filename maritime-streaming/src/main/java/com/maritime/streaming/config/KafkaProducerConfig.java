package com.maritime.streaming.config;

import com.maritime.common.observability.CorrelationIdProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value("${schema.registry.url:http://localhost:8085}")
    private String schemaRegistryUrl;

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    private Map<String, Object> producerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Idempotent producer: dedupes retries via PID + sequence numbers.
        // Combined with acks=all this guarantees no message loss within a partition.
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000L);
        // Avro serialization
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", schemaRegistryUrl);
        // Stamp the correlation-id header on every record so it survives the Kafka hop.
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                CorrelationIdProducerInterceptor.class.getName());
        return props;
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerProps());
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // Quarantine template: same factory (KafkaAvroSerializer), typed for VesselEvent.
    // Safe because KafkaAvroSerializer handles any SpecificRecord via Schema Registry.
    @Bean
    public KafkaTemplate<String, com.maritime.common.dto.VesselEvent> quarantineKafkaTemplate() {
        return new KafkaTemplate<>((ProducerFactory) producerFactory());
    }
}
