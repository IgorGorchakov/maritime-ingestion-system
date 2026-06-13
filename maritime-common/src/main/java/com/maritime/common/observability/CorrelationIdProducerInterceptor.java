package com.maritime.common.observability;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Stamps the {@code correlation-id} Kafka header on every outbound record so the id
 * survives the asynchronous hop to the next service.
 *
 * <p>This is a pure Kafka client interceptor (no Spring), wired via
 * {@code ProducerConfig.INTERCEPTOR_CLASSES_CONFIG}. The id is taken from the calling
 * thread's MDC — set either by {@link CorrelationIdHttpFilter} at a REST edge or by
 * {@link CorrelationIdRecordInterceptor} on a consumer thread that is re-producing.
 * If the thread has no id (e.g. a standalone scheduler that forgot to set one), a
 * fresh id is minted so the header is never empty.
 */
public class CorrelationIdProducerInterceptor implements ProducerInterceptor<Object, Object> {

    @Override
    public ProducerRecord<Object, Object> onSend(ProducerRecord<Object, Object> record) {
        // Respect an id the caller may have set on the record explicitly.
        if (record.headers().lastHeader(CorrelationIds.HEADER) == null) {
            String id = MDC.get(CorrelationIds.MDC_KEY);
            if (id == null) {
                id = CorrelationIds.newId();
            }
            record.headers().add(CorrelationIds.HEADER, id.getBytes(StandardCharsets.UTF_8));
        }
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // No-op: delivery success/failure is handled by the producer callbacks.
    }

    @Override
    public void close() {
        // No resources to release.
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // No configuration needed.
    }
}
