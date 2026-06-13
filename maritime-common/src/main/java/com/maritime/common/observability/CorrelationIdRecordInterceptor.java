package com.maritime.common.observability;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.kafka.listener.RecordInterceptor;

import java.nio.charset.StandardCharsets;

/**
 * Reads the {@code correlation-id} Kafka header off each consumed record and binds it
 * to the listener thread's MDC, so the id appears in every log line emitted while the
 * record is processed — and is picked up again by {@link CorrelationIdProducerInterceptor}
 * if the listener re-produces downstream.
 *
 * <p>The MDC is cleared in {@code afterRecord} (success and failure paths both route
 * through it) so a pooled listener thread never leaks one record's id onto the next.
 */
public class CorrelationIdRecordInterceptor implements RecordInterceptor<String, Object> {

    @Override
    public ConsumerRecord<String, Object> intercept(ConsumerRecord<String, Object> record,
                                                     Consumer<String, Object> consumer) {
        Header header = record.headers().lastHeader(CorrelationIds.HEADER);
        String id = header != null
                ? new String(header.value(), StandardCharsets.UTF_8)
                : CorrelationIds.newId();
        MDC.put(CorrelationIds.MDC_KEY, id);
        return record;
    }

    @Override
    public void afterRecord(ConsumerRecord<String, Object> record, Consumer<String, Object> consumer) {
        MDC.remove(CorrelationIds.MDC_KEY);
    }
}
