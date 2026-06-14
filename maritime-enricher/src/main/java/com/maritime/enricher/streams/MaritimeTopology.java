package com.maritime.enricher.streams;

import com.maritime.common.dto.EnrichedVesselEvent;
import com.maritime.common.kafka.Topics;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.time.Duration;
import java.util.Properties;

import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;

/**
 * Assembles and runs the Kafka Streams detection topology.
 *
 * <p>This class is responsible for <em>wiring</em> only: it registers the
 * RocksDB state store, connects the stream from {@code maritime.enriched}
 * through {@link VesselDetectionProcessor}, filters out clean records, and
 * routes flagged detections to {@code maritime.detections}. All detection
 * logic lives in {@link VesselDetectionProcessor}.
 *
 * <h3>Topic contract</h3>
 * <ul>
 *   <li><b>Input:</b> {@code maritime.enriched} — enriched events produced by
 *       {@code RiskScorerService}, consumed here under a dedicated group id
 *       ({@code maritime-detection-topology}) so the storage service's independent
 *       subscription to the same topic is not affected.</li>
 *   <li><b>Output:</b> {@code maritime.detections} — a <em>separate</em> topic;
 *       never the input topic. Writing detections back to {@code maritime.enriched}
 *       would create a processing loop. The separate topic makes the data flow a
 *       strict DAG.</li>
 * </ul>
 *
 * <h3>Serde lifecycle</h3>
 * {@link #consumeSerde} and {@link #produceSerde} are constructed in the constructor
 * (not inside {@link #buildTopology()}) and closed in {@link #stop()} after
 * {@code streams.close()} returns. This ensures the {@code SchemaRegistryClient}
 * HTTP connection pools they own are drained exactly once, after all in-flight
 * serialization has completed. See {@link AvroEnrichedEventSerdes} for the full
 * explanation.
 */
@Slf4j
@Component
public class MaritimeTopology {

    static final String STATE_STORE = "vessel-state-store";
    static final String INPUT_TOPIC = Topics.ENRICHED;
    static final String OUTPUT_TOPIC = Topics.DETECTIONS;

    private final MeterRegistry meterRegistry;
    private final String bootstrapServers;
    private final String schemaRegistryUrl;

    private final AvroEnrichedEventSerdes consumeSerde;
    private final AvroEnrichedEventSerdes produceSerde;

    private KafkaStreams streams;

    public MaritimeTopology(
            MeterRegistry meterRegistry,
            @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers,
            @Value("${schema.registry.url:http://localhost:8085}") String schemaRegistryUrl) {
        this.meterRegistry = meterRegistry;
        this.bootstrapServers = bootstrapServers;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.consumeSerde = new AvroEnrichedEventSerdes(schemaRegistryUrl);
        this.produceSerde = new AvroEnrichedEventSerdes(schemaRegistryUrl);
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @PostConstruct
    public void start() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "maritime-detection-topology");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/maritime");
        props.put(StreamsConfig.POLL_MS_CONFIG, 5_000);

        streams = new KafkaStreams(buildTopology(), props);
        streams.setUncaughtExceptionHandler(ex -> {
            log.error("Uncaught exception in Kafka Streams topology", ex);
            return REPLACE_THREAD;
        });
        streams.start();
        log.info("MaritimeTopology started — consuming {} producing {}", INPUT_TOPIC, OUTPUT_TOPIC);
    }

    @PreDestroy
    public void stop() {
        if (streams != null) {
            streams.close(Duration.ofSeconds(10));
            log.info("MaritimeTopology stopped");
        }
        consumeSerde.close();
        produceSerde.close();
        log.info("MaritimeTopology serdes closed");
    }

    // ── Topology ──────────────────────────────────────────────────────────────

    private Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        // RocksDB state store — one VesselState per MMSI, fault-tolerant via changelog.
        StoreBuilder<KeyValueStore<String, VesselState>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STATE_STORE),
                        Serdes.String(),
                        new VesselStateSerdes()
                );
        builder.addStateStore(storeBuilder);

        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), consumeSerde))
                // VesselDetectionProcessor updates state and forwards only when a flag fires.
                .process(() -> new VesselDetectionProcessor(meterRegistry), STATE_STORE)
                // null values mean no flag fired — drop them before the output topic.
                .filter((key, value) -> value != null)
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), produceSerde));

        return builder.build();
    }
}
