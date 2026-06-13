package com.maritime.streaming.config;

import com.maritime.common.observability.CorrelationIdRecordInterceptor;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${schema.registry.url:http://localhost:8085}")
    private String schemaRegistryUrl;

    @Value("${spring.kafka.consumer.group-id:streaming-service}")
    private String groupId;

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    // ---- Consumer factory ----

    private Map<String, Object> consumerProps(String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("specific.avro.reader", true);
        return props;
    }

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = consumerProps(groupId);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    // ---- Listener container factory with MANUAL_IMMEDIATE acks + DLQ ----

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            KafkaTemplate<String, Object> kafkaTemplate,
            CorrelationIdRecordInterceptor correlationIdRecordInterceptor,
            MeterRegistry meterRegistry) {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        // Bind the correlation id off each record's header into MDC before the listener
        // runs, so log lines and any re-produced records carry the same id end-to-end.
        factory.setRecordInterceptor(correlationIdRecordInterceptor);

        // MANUAL_IMMEDIATE: offset committed only after listener returns successfully.
        // This gives at-least-once semantics — if the listener throws, the offset
        // is NOT committed and the record will be re-delivered.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // Count every record diverted to a DLT — a non-zero rate is an alarm signal.
        Counter dlqCounter = Counter.builder("dlq")
                .description("Records routed to a dead-letter topic after exhausting retries")
                .register(meterRegistry);

        // DLQ: poison records go to <topic>.DLT instead of blocking the partition.
        // Fixed backoff: 3 retries at 1s intervals, then send to DLT.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> {
                    dlqCounter.increment();
                    return new TopicPartition(record.topic() + ".DLT", 0);
                });
        factory.setCommonErrorHandler(new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3)));

        return factory;
    }

    // ---- Topic admin: declare DLT topics explicitly ----

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", bootstrapServers);
        return new KafkaAdmin(props);
    }
}
