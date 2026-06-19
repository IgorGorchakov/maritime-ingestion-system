package com.maritime.enricher.config;

import com.maritime.common.observability.CorrelationIdRecordInterceptor;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka consumer configuration for the enricher service.
 *
 * <p>Configures one listener container factory used by
 * {@link com.maritime.enricher.service.RiskScorerEnrichService} for the
 * {@code maritime.ais.raw} topic. The factory sets:
 * <ul>
 *   <li>{@code MANUAL_IMMEDIATE} ack mode — offsets are committed only after the
 *       listener returns successfully, giving at-least-once semantics.</li>
 *   <li>A {@link DeadLetterPublishingRecoverer} — after 3 retries at 1 s
 *       intervals, poison records are routed to {@code <topic>.DLT} rather than
 *       blocking the partition forever.</li>
 *   <li>A {@link CorrelationIdRecordInterceptor} — binds the correlation ID from
 *       the record header into MDC before the listener runs, so every log line
 *       produced during processing carries the same trace ID.</li>
 * </ul>
 *
 * <p>This class has no knowledge of Kafka Streams. Stream processing lives
 * entirely in the {@code maritime-detection} module.
 */
@Configuration
public class KafkaConsumerConfig {

    @Value("${schema.registry.url:http://localhost:8085}")
    private String schemaRegistryUrl;

    /** Consumer group for RiskScorerEnrichService — must match @KafkaListener(groupId). */
    @Value("${spring.kafka.consumer.group-id:enricher-service}")
    private String groupId;

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    private Map<String, Object> consumerProps(String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,       bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,       "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,  StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put("schema.registry.url",                         schemaRegistryUrl);
        props.put("specific.avro.reader",                        true);
        return props;
    }

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerProps(groupId));
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
        factory.setRecordInterceptor(correlationIdRecordInterceptor);

        // MANUAL_IMMEDIATE: offset committed only after the listener returns without
        // throwing. If the listener throws, the offset is NOT committed and Kafka
        // redelivers the record — at-least-once semantics.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // DLQ counter — a non-zero rate is an operational alarm signal.
        Counter dlqCounter = Counter.builder("dlq")
                .description("Records routed to a dead-letter topic after exhausting retries")
                .register(meterRegistry);

        // After 3 retries at 1 s intervals, send the record to <topic>.DLT so
        // processing of other records on the same partition can continue.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> {
                    dlqCounter.increment();
                    return new TopicPartition(record.topic() + ".DLT", 0);
                });
        factory.setCommonErrorHandler(new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3)));

        return factory;
    }

    @Bean
    public KafkaAdmin kafkaAdmin() {
        return new KafkaAdmin(Map.of("bootstrap.servers", bootstrapServers));
    }
}
