package com.maritime.storage.config;

import com.maritime.common.observability.CorrelationIdRecordInterceptor;
import com.maritime.storage.config.properties.StorageKafkaProperties;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka consumer configuration for the storage service.
 *
 * <p>Configures the listener container factory used by
 * {@link com.maritime.storage.service.VesselEventConsumer} for the enriched and
 * detection topics. Ack mode is {@code MANUAL_IMMEDIATE} for at-least-once semantics;
 * a {@link DeadLetterPublishingRecoverer} routes poison records to {@code <topic>.DLT}
 * after 3 retries at 1 s intervals.
 *
 * <p>All tunable values are injected via {@link StorageKafkaProperties} — a typed
 * {@code @ConfigurationProperties} record whose defaults live only in
 * {@code application.properties} (audit H4). The previous {@code @Value} inline
 * defaults are removed.
 */
@Configuration
public class KafkaConsumerConfig {

    private final StorageKafkaProperties kafka;

    public KafkaConsumerConfig(StorageKafkaProperties kafka) {
        this.kafka = kafka;
    }

    private Map<String, Object> consumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        kafka.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                 kafka.groupId());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put("schema.registry.url",                          kafka.schemaRegistryUrl());
        props.put("specific.avro.reader",                         true);
        return props;
    }

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerProps());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            KafkaTemplate<String, Object> kafkaTemplate,
            CorrelationIdRecordInterceptor correlationIdRecordInterceptor,
            MeterRegistry meterRegistry) {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        // Bind the correlation ID off each record header into MDC before the listener
        // runs, so log lines carry the same ID that originated upstream at ingestion.
        factory.setRecordInterceptor(correlationIdRecordInterceptor);

        // MANUAL_IMMEDIATE: offset committed only after listener returns successfully.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // Count every record diverted to a DLT — a non-zero rate is an alarm signal.
        Counter dlqCounter = Counter.builder("dlq")
                .description("Records routed to a dead-letter topic after exhausting retries")
                .register(meterRegistry);

        // DLQ: poison records go to <topic>.DLT instead of blocking the partition.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> {
                    dlqCounter.increment();
                    return new TopicPartition(record.topic() + ".DLT", 0);
                });
        factory.setCommonErrorHandler(new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3)));

        return factory;
    }
}
