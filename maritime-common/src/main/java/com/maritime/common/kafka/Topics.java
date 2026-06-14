package com.maritime.common.kafka;

/**
 * Single source of truth for every Kafka topic name in the platform.
 *
 * <p>Topic names were previously duplicated as string literals across the
 * ingestion, streaming, and storage services (and their tests). A typo in any
 * one of them silently breaks the producer/consumer contract. Centralising the
 * names here means there is exactly one place to change a topic and the compiler
 * enforces consistency everywhere it is referenced.
 *
 * <h3>Why these are compile-time constants</h3>
 * Every field is a {@code public static final String} initialised from a string
 * literal (or a concatenation of constants). That makes each one a Java
 * <em>compile-time constant</em>, so the values are legal inside annotation
 * attributes such as {@code @KafkaListener(topics = Topics.ENRICHED)} — which
 * require constants. Do <strong>not</strong> change these to method calls or
 * runtime-built strings, or annotation usages will stop compiling.
 */
public final class Topics {

    private Topics() {
        // Constants holder — never instantiated.
    }

    /** Suffix appended to a topic to form its dead-letter topic. */
    public static final String DLT_SUFFIX = ".DLT";

    /** Raw AIS reports produced by the ingestion service. */
    public static final String AIS_RAW = "maritime.ais.raw";

    /** Enriched events (zone + risk) produced by the streaming ETL. */
    public static final String ENRICHED = "maritime.enriched";

    /** Detection events (loitering / dark vessel / speed anomaly) from the topology. */
    public static final String DETECTIONS = "maritime.detections";

    /** Invalid or duplicate events routed aside for audit. */
    public static final String QUARANTINE = "maritime.ais.quarantine";

    /** Dead-letter topic for poison records on {@link #AIS_RAW}. */
    public static final String AIS_RAW_DLT = AIS_RAW + DLT_SUFFIX;

    /** Dead-letter topic for poison records on {@link #ENRICHED}. */
    public static final String ENRICHED_DLT = ENRICHED + DLT_SUFFIX;
}
