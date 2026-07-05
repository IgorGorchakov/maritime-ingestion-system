package com.maritime.detection.streams;

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
import org.apache.kafka.streams.kstream.KStream;
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
 * Assembles and runs the Kafka Streams detection topology for the
 * {@code maritime-detection} service (port 8086).
 *
 * <p>Responsible for <em>wiring</em> only: registers the RocksDB state store,
 * connects the stream from {@code maritime.enriched} through
 * {@link VesselDetectionProcessor}, and routes flagged events to
 * {@code maritime.detections}. All detection logic lives in
 * {@link VesselDetectionProcessor}.
 *
 * <h3>Topic contract</h3>
 * <ul>
 *   <li><b>Input:</b> {@code maritime.enriched} — produced by
 *       {@code RiskScorerEnrichService} in {@code maritime-enricher}. Consumed here
 *       under group id {@code maritime-detection-topology}, independently of the
 *       storage service's own subscription to the same topic.</li>
 *   <li><b>Output:</b> {@code maritime.detections} — a separate topic; never the
 *       input topic. Writing detections back to {@code maritime.enriched} would
 *       create a processing loop. The separate topic keeps the data flow a strict
 *       DAG. Topic ownership and declaration live in {@link com.maritime.detection.config.DetectionTopicConfig}.</li>
 * </ul>
 *
 * <h3>Serde lifecycle</h3>
 * {@link #consumeSerde} and {@link #produceSerde} are constructed once in the
 * constructor and closed in {@link #stop()} only after {@code KafkaStreams.close()}
 * returns. This ensures the {@code SchemaRegistryClient} HTTP connection pools they
 * own are released only after all in-flight serialization has completed.
 *
 * <h3>State directory</h3>
 * RocksDB writes state to {@code kafka.streams.state-dir} (default
 * {@code /tmp/kafka-streams/maritime}). Override via the
 * {@code KAFKA_STREAMS_STATE_DIR} environment variable or the
 * {@code kafka.streams.state-dir} application property to avoid collisions when
 * running multiple instances on the same host.
 */
@Slf4j
@Component
public class MaritimeTopology {

    static final String STATE_STORE = "vessel-state-store";

    private final MeterRegistry           meterRegistry;
    private final String                  bootstrapServers;
    private final String                  schemaRegistryUrl;
    /** RocksDB state directory — injectable so tests and multi-instance deployments
     *  can override it without a code change. */
    private final String                  stateDir;
    private final AvroEnrichedEventSerdes      consumeSerde;
    private final AvroEnrichedEventSerdes      produceSerde;
    private final AvroHexCrossingEventSerdes   hexCrossingSerde;

    private KafkaStreams streams;

    public MaritimeTopology(
            MeterRegistry meterRegistry,
            @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers,
            @Value("${schema.registry.url:http://localhost:8085}")     String schemaRegistryUrl,
            @Value("${kafka.streams.state-dir:/tmp/kafka-streams/maritime}") String stateDir) {
        this.meterRegistry      = meterRegistry;
        this.bootstrapServers   = bootstrapServers;
        this.schemaRegistryUrl  = schemaRegistryUrl;
        this.stateDir           = stateDir;
        this.consumeSerde       = new AvroEnrichedEventSerdes(schemaRegistryUrl);
        this.produceSerde       = new AvroEnrichedEventSerdes(schemaRegistryUrl);
        this.hexCrossingSerde   = new AvroHexCrossingEventSerdes(schemaRegistryUrl);
    }

    @PostConstruct
    public void start() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,            "maritime-detection-topology");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,         bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,   Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        // stateDir is injected via @Value — Spring resolves the property before it
        // reaches the constructor, so this is a real path, not a placeholder string.
        props.put(StreamsConfig.STATE_DIR_CONFIG,                 stateDir);
        props.put(StreamsConfig.POLL_MS_CONFIG,                   5_000);

        streams = new KafkaStreams(buildTopology(), props);
        streams.setUncaughtExceptionHandler(ex -> {
            log.error("Uncaught exception in Kafka Streams topology", ex);
            return REPLACE_THREAD;
        });
        streams.start();
        log.info("MaritimeTopology started — consuming={} producing={} stateDir={}",
                Topics.ENRICHED, Topics.DETECTIONS, stateDir);
    }

    @PreDestroy
    public void stop() {
        if (streams != null) {
            streams.close(Duration.ofSeconds(10));
            log.info("MaritimeTopology stopped");
        }
        // Close serdes only after streams.close() returns, so no in-flight
        // serialization is interrupted when the connection pool is released.
        consumeSerde.close();
        produceSerde.close();
        hexCrossingSerde.close();
        log.info("MaritimeTopology serdes closed");
    }

    Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        // ── Existing vessel-state store ───────────────────────────────────────
        StoreBuilder<KeyValueStore<String, VesselState>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STATE_STORE),
                        Serdes.String(),
                        new VesselStateSerdes());
        builder.addStateStore(storeBuilder);

        // ── New hex-cell store (mmsi → last H3 cell address) ─────────────────
        StoreBuilder<KeyValueStore<String, String>> hexStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(HexCrossingProcessor.HEX_STATE_STORE),
                        Serdes.String(),
                        Serdes.String());
        builder.addStateStore(hexStoreBuilder);

        // ── Shared source stream ──────────────────────────────────────────────
        KStream<String, EnrichedVesselEvent> source =
                builder.stream(Topics.ENRICHED, Consumed.with(Serdes.String(), consumeSerde));

        // ── Branch 1: existing behavioural detectors ──────────────────────────
        source.process(() -> new VesselDetectionProcessor(meterRegistry), STATE_STORE)
              .filter((key, value) -> value != null)
              .to(Topics.DETECTIONS, Produced.with(Serdes.String(), produceSerde));

        // ── Branch 2: H3 hex cell crossing detector ───────────────────────────
        source.process(() -> new HexCrossingProcessor(), HexCrossingProcessor.HEX_STATE_STORE)
              .to(Topics.HEX_CROSSINGS, Produced.with(Serdes.String(), hexCrossingSerde));

        return builder.build();
    }
}
