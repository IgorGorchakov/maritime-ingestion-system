package com.maritime.streaming.streams;

import com.maritime.common.dto.EnrichedVesselEvent;
import com.maritime.common.geo.GeoUtils;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

/**
 * Kafka Streams topology for stateful per-vessel behavioural detection.
 *
 * <h3>Design decisions</h3>
 * <ul>
 *   <li><b>State store (RocksDB):</b> per-MMSI VesselState survives restarts via
 *       the internal changelog topic. We use a KeyValueStore (not a windowed store)
 *       because detectors need the full previous snapshot, not just a count.</li>
 *   <li><b>Input:</b> {@code maritime.enriched} — we run detection on already-enriched
 *       events so the zone/risk context is available and we don't duplicate the
 *       validation/dedup work that RiskScorerService already does.</li>
 *   <li><b>Output:</b> detections (events where at least one flag fired) are
 *       published to {@code maritime.detections}, a dedicated topic separate from
 *       the input. This makes the data flow a strict DAG — the topology never
 *       consumes its own output and no processing loop is possible.
 *       The storage service subscribes to both topics independently.</li>
 *   <li><b>Punctuator:</b> dark-vessel detection can't rely on an incoming record —
 *       it fires when records *stop*. We schedule a wall-clock punctuator that scans
 *       the state store every minute and flags vessels silent for >= 10 minutes.</li>
 * </ul>
 *
 * <h3>Detectors</h3>
 * <ol>
 *   <li><b>Speed anomaly:</b> compare Haversine-implied speed (from previous position)
 *       against reported SOG. Flag if divergence > {@code SPEED_ANOMALY_THRESHOLD_KN}.</li>
 *   <li><b>Loitering:</b> vessel has been moving at < {@code LOITER_SPEED_KN} knots for
 *       >= {@code LOITER_DURATION_MINUTES} consecutive minutes.</li>
 *   <li><b>Dark vessel:</b> no AIS report received for >= {@code DARK_VESSEL_MINUTES}.</li>
 * </ol>
 */
@Slf4j
@Component
public class MaritimeTopology {

    // ── Tuning constants ────────────────────────────────────────────────────
    /** Vessels below this speed (knots) for long enough are flagged as loitering. */
    static final double LOITER_SPEED_KN        = 1.0;
    /** How long (minutes) of low-speed dwell before loitering is declared. */
    static final long   LOITER_DURATION_MINUTES = 5L;
    /** Minutes of silence before a vessel is flagged as dark/AIS-gap. */
    static final long   DARK_VESSEL_MINUTES     = 10L;
    /** Knot divergence between implied and reported speed to flag a speed anomaly. */
    static final double SPEED_ANOMALY_THRESHOLD_KN = 15.0;
    /** Nautical-miles-per-meter conversion. */
    private static final double METERS_TO_NM = 1.0 / 1852.0;

    static final String STATE_STORE  = "vessel-state-store";
    static final String INPUT_TOPIC   = "maritime.enriched";
    /**
     * Detections are published to a dedicated output topic, NOT back to the
     * input topic. Writing to maritime.enriched would create a processing loop:
     * the topology would consume its own re-published records and the storage
     * service would double-persist every detected event. A separate topic
     * makes the data flow a strict DAG — structurally impossible to loop.
     *
     * Consumers of maritime.detections (storage service) subscribe alongside
     * maritime.enriched so they receive both clean enriched events and flagged
     * detection events independently.
     */
    static final String OUTPUT_TOPIC  = "maritime.detections";

    private final MeterRegistry meterRegistry;
    private final String bootstrapServers;
    private final String schemaRegistryUrl;
    private KafkaStreams streams;

    public MaritimeTopology(
            MeterRegistry meterRegistry,
            @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers,
            @Value("${schema.registry.url:http://localhost:8085}") String schemaRegistryUrl) {
        this.meterRegistry    = meterRegistry;
        this.bootstrapServers = bootstrapServers;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @PostConstruct
    public void start() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,    "maritime-detection-topology");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Internal changelog topics for the state store use String keys + JSON values.
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,   Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        // State store directory — survives service restarts without reprocessing the log.
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/maritime");
        // Poll interval: how often the punctuator thread wakes up.
        props.put(StreamsConfig.POLL_MS_CONFIG, 5_000);

        Topology topology = buildTopology();
        streams = new KafkaStreams(topology, props);

        streams.setUncaughtExceptionHandler(ex -> {
            log.error("Uncaught exception in Kafka Streams topology", ex);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        streams.start();
        log.info("MaritimeTopology started (input={}, output={})", INPUT_TOPIC, OUTPUT_TOPIC);
    }

    @PreDestroy
    public void stop() {
        if (streams != null) {
            streams.close(Duration.ofSeconds(10));
            log.info("MaritimeTopology stopped");
        }
    }

    // ── Topology definition ──────────────────────────────────────────────────

    Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        // State store: one VesselState record per MMSI, persisted in RocksDB.
        // The changelog topic makes it fault-tolerant across restarts.
        StoreBuilder<KeyValueStore<String, VesselState>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STATE_STORE),
                        Serdes.String(),
                        new VesselStateSerdes()
                );
        builder.addStateStore(storeBuilder);

        // Read enriched events. The value Serde must match KafkaAvroDeserializer
        // because RiskScorerService wrote Avro EnrichedVesselEvent records.
        // We consume as byte[] and let the Processor handle Avro via the registry.
        // However, since spring-kafka already configured the Avro deserializer for
        // the main consumer group, we use a separate Streams consumer group here.
        //
        // Streams reads from the SAME topic as the storage service, but with a
        // DIFFERENT group id ("maritime-detection-topology") so both receive all
        // records independently.
        builder.stream(INPUT_TOPIC,
                        Consumed.with(Serdes.String(), new AvroEnrichedEventSerdes(schemaRegistryUrl)))
                .process(detectionProcessorSupplier(), STATE_STORE)
                // Filter: only forward records where at least one detection fired.
                // Records with no flags set produce null from the processor and
                // are dropped here — no output to maritime.detections for clean events.
                .filter((key, value) -> value != null)
                .to(OUTPUT_TOPIC,
                        Produced.with(Serdes.String(), new AvroEnrichedEventSerdes(schemaRegistryUrl)));

        return builder.build();
    }

    // ── Detection processor ──────────────────────────────────────────────────

    private ProcessorSupplier<String, EnrichedVesselEvent, String, EnrichedVesselEvent>
    detectionProcessorSupplier() {
        return () -> new Processor<>() {

            private KeyValueStore<String, VesselState> store;
            private ProcessorContext<String, EnrichedVesselEvent> ctx;

            @Override
            public void init(ProcessorContext<String, EnrichedVesselEvent> context) {
                this.ctx   = context;
                this.store = context.getStateStore(STATE_STORE);

                // Dark vessel punctuator: fire every minute on wall-clock time,
                // scan the store for vessels silent >= DARK_VESSEL_MINUTES.
                context.schedule(Duration.ofMinutes(1), PunctuationType.WALL_CLOCK_TIME,
                        timestamp -> checkDarkVessels(timestamp));
            }

            @Override
            public void process(Record<String, EnrichedVesselEvent> record) {
                EnrichedVesselEvent event = record.value();
                if (event == null) return;

                String mmsi = event.getVesselEvent().getMmsi();
                VesselState prev = store.get(mmsi);

                boolean loitering    = detectLoitering(prev, event);
                boolean speedAnomaly = detectSpeedAnomaly(prev, event);

                // Build updated state for this vessel.
                long nowMs = event.getVesselEvent().getTimestamp().toEpochMilli();
                VesselState current = new VesselState(
                        mmsi,
                        event.getVesselEvent().getLatitude(),
                        event.getVesselEvent().getLongitude(),
                        event.getVesselEvent().getSpeed(),
                        nowMs
                );
                current.setLoitering(loitering);
                current.setSpeedAnomaly(speedAnomaly);
                store.put(mmsi, current);

                // Only re-publish if at least one detection flag fired, to avoid
                // creating an infinite re-publish loop for clean events.
                if (loitering || speedAnomaly) {
                    if (loitering)    meterRegistry.counter("detections", "type", "loitering").increment();
                    if (speedAnomaly) meterRegistry.counter("detections", "type", "speed_anomaly").increment();

                    EnrichedVesselEvent flagged = setDetectionFlags(event, loitering, false, speedAnomaly);
                    log.warn("Detection fired for MMSI {}: loitering={} speedAnomaly={}",
                            mmsi, loitering, speedAnomaly);
                    ctx.forward(record.withValue(flagged));
                }
            }

            // ── Loitering ────────────────────────────────────────────────────
            // A vessel is loitering when it has been below LOITER_SPEED_KN for at
            // least LOITER_DURATION_MINUTES. We track continuous low-speed time via
            // the elapsed wall-clock gap between consecutive reports.
            private boolean detectLoitering(VesselState prev, EnrichedVesselEvent event) {
                if (prev == null) return false;
                double speed = event.getVesselEvent().getSpeed();
                if (speed >= LOITER_SPEED_KN) return false;   // moving normally

                long nowMs  = event.getVesselEvent().getTimestamp().toEpochMilli();
                long gapMin = (nowMs - prev.getLastSeenMs()) / 60_000L;

                // Vessel was also slow in the previous report and has been drifting
                // for long enough → declare loitering.
                return prev.getSpeed() < LOITER_SPEED_KN
                        && gapMin >= LOITER_DURATION_MINUTES;
            }

            // ── Speed anomaly ─────────────────────────────────────────────────
            // Derive implied speed from consecutive Haversine distance / time delta
            // and compare against reported SOG. A large divergence suggests spoofing
            // or sensor malfunction.
            private boolean detectSpeedAnomaly(VesselState prev, EnrichedVesselEvent event) {
                if (prev == null) return false;

                long nowMs   = event.getVesselEvent().getTimestamp().toEpochMilli();
                long elapsedMs = nowMs - prev.getLastSeenMs();
                if (elapsedMs <= 0) return false;

                double distMeters = GeoUtils.calculateDistanceInMeters(
                        prev.getLatitude(), prev.getLongitude(),
                        event.getVesselEvent().getLatitude(),
                        event.getVesselEvent().getLongitude()
                );
                double distNm      = distMeters * METERS_TO_NM;
                double elapsedHrs  = elapsedMs / 3_600_000.0;
                double impliedKnots = distNm / elapsedHrs;
                double reportedKnots = event.getVesselEvent().getSpeed();

                double divergence = Math.abs(impliedKnots - reportedKnots);
                if (divergence > SPEED_ANOMALY_THRESHOLD_KN) {
                    log.debug("Speed anomaly MMSI {}: implied={:.1f}kn reported={:.1f}kn diff={:.1f}",
                            event.getVesselEvent().getMmsi(), impliedKnots, reportedKnots, divergence);
                    return true;
                }
                return false;
            }

            // ── Dark vessel (punctuator) ───────────────────────────────────────
            // Scans the full state store on a wall-clock schedule. Any vessel whose
            // lastSeenMs is older than DARK_VESSEL_MINUTES is flagged as dark.
            // We cannot detect silence via stream processing alone — silence produces
            // no records, so we need a scheduled side-channel check.
            private void checkDarkVessels(long wallClockMs) {
                long thresholdMs = wallClockMs - (DARK_VESSEL_MINUTES * 60_000L);
                try (KeyValueIterator<String, VesselState> it = store.all()) {
                    while (it.hasNext()) {
                        var entry = it.next();
                        VesselState state = entry.value;
                        if (state == null) continue;

                        boolean wasDark = state.isDarkVessel();
                        boolean isDark  = state.getLastSeenMs() < thresholdMs;

                        if (isDark && !wasDark) {
                            // Transition to dark: flag the stored state and emit a synthetic
                            // detection record so storage/gateway can surface it.
                            state.setDarkVessel(true);
                            store.put(entry.key, state);
                            meterRegistry.counter("detections", "type", "dark_vessel").increment();
                            log.warn("Dark vessel detected: MMSI {} last seen {}",
                                    state.getMmsi(),
                                    Instant.ofEpochMilli(state.getLastSeenMs()));
                            // We can't forward a new Avro record from a punctuator without the
                            // original record, so we just log + increment the metric here.
                            // Phase 8 can expose this via the MCP get_dark_vessels tool.
                        } else if (!isDark && wasDark) {
                            // Vessel came back online — clear the flag.
                            state.setDarkVessel(false);
                            store.put(entry.key, state);
                        }
                    }
                }
            }
        };
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /**
     * Return a new EnrichedVesselEvent with the detection flags set.
     * Avro SpecificRecords are effectively immutable (all fields set at build time),
     * so we copy via the builder, carrying over all existing fields.
     */
    static EnrichedVesselEvent setDetectionFlags(EnrichedVesselEvent src,
                                                  boolean loitering,
                                                  boolean darkVessel,
                                                  boolean speedAnomaly) {
        return EnrichedVesselEvent.newBuilder()
                .setVesselEvent(src.getVesselEvent())
                .setInRestrictedZone(src.getInRestrictedZone())
                .setZoneName(src.getZoneName())
                .setZoneType(src.getZoneType())
                .setDistanceToPort(src.getDistanceToPort())
                .setRiskScore(src.getRiskScore())
                .setRiskLevel(src.getRiskLevel())
                .setLoitering(loitering)
                .setDarkVessel(darkVessel)
                .setSpeedAnomaly(speedAnomaly)
                .build();
    }
}
