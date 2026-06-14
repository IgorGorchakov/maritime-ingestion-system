package com.maritime.common.observability;

import java.util.UUID;

/**
 * Shared correlation-id conventions used to trace a single vessel event across
 * the four services (ingestion → streaming → storage, plus the api service).
 *
 * There is no synchronous HTTP call chain to carry trace context across the
 * asynchronous Kafka hops, so the id is generated once at the edge and then
 * propagated on a Kafka record header and mirrored into SLF4J's MDC so it lands
 * in every log line.
 *
 * Named {@code CorrelationIds} (not {@code KafkaHeaders}) to avoid colliding with
 * {@link org.springframework.kafka.support.KafkaHeaders}.
 */
public final class CorrelationIds {

    /** Kafka record header carrying the correlation id between services. */
    public static final String HEADER = "correlation-id";

    /** Inbound/outbound HTTP header for the REST entrypoints (ingestion, api). */
    public static final String HTTP_HEADER = "X-Correlation-Id";

    /** MDC key; referenced by the logback pattern as {@code %X{correlationId}}. */
    public static final String MDC_KEY = "correlationId";

    private CorrelationIds() {
    }

    /** Generate a fresh correlation id. */
    public static String newId() {
        return UUID.randomUUID().toString();
    }
}
