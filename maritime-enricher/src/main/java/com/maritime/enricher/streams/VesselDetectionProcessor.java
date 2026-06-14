package com.maritime.enricher.streams;

import com.maritime.common.dto.EnrichedVesselEvent;
import com.maritime.common.geo.GeoUtils;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;

/**
 * Kafka Streams {@link Processor} that runs three behavioural detectors against
 * per-vessel state held in a RocksDB {@link KeyValueStore}.
 *
 * <h3>Detectors</h3>
 * <ol>
 *   <li><b>Loitering</b> — vessel has been below {@link #LOITER_SPEED_KN} knots for
 *       at least {@link #LOITER_DURATION_MINUTES} consecutive minutes, measured via the
 *       elapsed wall-clock gap between consecutive AIS reports.</li>
 *   <li><b>Speed anomaly</b> — Haversine-implied speed (distance / elapsed time between
 *       the previous and current position) diverges from reported SOG by more than
 *       {@link #SPEED_ANOMALY_THRESHOLD_KN} knots. A large divergence suggests AIS
 *       position spoofing or a sensor fault.</li>
 *   <li><b>Dark vessel</b> — no AIS report received for {@link #DARK_VESSEL_MINUTES}
 *       minutes. Silence cannot be detected from the stream itself, so a wall-clock
 *       {@link org.apache.kafka.streams.processor.Punctuator} scans the full state
 *       store every minute and flags vessels whose {@code lastSeenMs} has crossed the
 *       threshold.</li>
 * </ol>
 *
 * <h3>Output contract</h3>
 * The processor forwards a record downstream <em>only</em> when at least one flag fires.
 * Clean records (no flags) return without a {@code ctx.forward()} call, which lets the
 * topology's {@code .filter((k, v) -> v != null)} drop them before the DETECTIONS topic.
 * The forwarded record is a copy of the input with the detection flags set — the original
 * {@link EnrichedVesselEvent} is never mutated (Avro SpecificRecords are rebuilt via
 * {@link #withFlags}).
 *
 * <h3>State store ownership</h3>
 * The store name ({@link MaritimeTopology#STATE_STORE}) is registered in
 * {@link MaritimeTopology#buildTopology()} and passed to {@code .process(..., STATE_STORE)}.
 * This processor retrieves it by name in {@link #init}. The store lifecycle (open, close,
 * changelog replication) is fully managed by Kafka Streams.
 *
 * <h3>Thread safety</h3>
 * Kafka Streams guarantees that a single processor instance is accessed by exactly one
 * stream thread at a time. No synchronisation is required here.
 */
@Slf4j
public class VesselDetectionProcessor implements Processor<String, EnrichedVesselEvent, String, EnrichedVesselEvent> {

    // ── Tuning constants ──────────────────────────────────────────────────────
    /**
     * Vessels below this speed (knots) for long enough are flagged as loitering.
     */
    static final double LOITER_SPEED_KN = 1.0;
    /**
     * Consecutive minutes at low speed before loitering is declared.
     */
    static final long LOITER_DURATION_MINUTES = 5L;
    /**
     * Minutes of AIS silence before a vessel is flagged as dark.
     */
    static final long DARK_VESSEL_MINUTES = 10L;
    /**
     * Knot divergence between Haversine-implied and reported SOG to flag an anomaly.
     */
    static final double SPEED_ANOMALY_THRESHOLD_KN = 15.0;

    private static final double METERS_TO_NM = 1.0 / 1852.0;

    // ── Dependencies ──────────────────────────────────────────────────────────
    private final MeterRegistry meterRegistry;

    // ── Streams-managed state ─────────────────────────────────────────────────
    private KeyValueStore<String, VesselState> store;
    private ProcessorContext<String, EnrichedVesselEvent> ctx;

    /**
     * @param meterRegistry injected by {@link MaritimeTopology} so detection counters
     *                      are registered on the application's shared registry.
     */
    public VesselDetectionProcessor(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    // ── Processor lifecycle ───────────────────────────────────────────────────

    /**
     * Build the {@link VesselState} to persist after processing this record.
     * Always written to the store so {@code lastSeenMs} stays current even for
     * clean (no-flag) events — the dark-vessel punctuator depends on it.
     */
    private static VesselState updatedState(String mmsi,
                                            EnrichedVesselEvent event,
                                            boolean loitering,
                                            boolean speedAnomaly) {
        long nowMs = event.getVesselEvent().getTimestamp().toEpochMilli();
        VesselState s = new VesselState(
                mmsi,
                event.getVesselEvent().getLatitude(),
                event.getVesselEvent().getLongitude(),
                event.getVesselEvent().getSpeed(),
                nowMs
        );
        s.setLoitering(loitering);
        s.setSpeedAnomaly(speedAnomaly);
        return s;
    }

    /**
     * Return a copy of {@code src} with the three detection flags set.
     *
     * <p>Avro {@code SpecificRecord} instances are effectively immutable once built
     * (all fields assigned at build time, no setters on generated classes). We use
     * the generated builder to copy every existing field and override only the flags,
     * which is both correct and exhaustive — adding a new field to the schema will
     * cause a compile error here if the builder call is not updated.
     */
    static EnrichedVesselEvent withFlags(EnrichedVesselEvent src,
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

    @Override
    public void init(ProcessorContext<String, EnrichedVesselEvent> context) {
        this.ctx = context;
        this.store = context.getStateStore(MaritimeTopology.STATE_STORE);

        // Schedule the dark-vessel scan on wall-clock time so it fires even when
        // no records are arriving. STREAM_TIME would never advance during silence,
        // which is exactly the condition we need to detect.
        context.schedule(
                Duration.ofMinutes(1),
                PunctuationType.WALL_CLOCK_TIME,
                this::checkDarkVessels
        );

        log.info("VesselDetectionProcessor initialised, state store={}", MaritimeTopology.STATE_STORE);
    }

    // ── Detectors ─────────────────────────────────────────────────────────────

    @Override
    public void process(Record<String, EnrichedVesselEvent> record) {
        EnrichedVesselEvent event = record.value();
        if (event == null) return;

        String mmsi = event.getVesselEvent().getMmsi();
        VesselState prev = store.get(mmsi);

        boolean loitering = detectLoitering(prev, event);
        boolean speedAnomaly = detectSpeedAnomaly(prev, event);

        // Persist the updated state regardless of whether a flag fired.
        // This keeps lastSeenMs current for the dark-vessel punctuator.
        store.put(mmsi, updatedState(mmsi, event, loitering, speedAnomaly));

        if (loitering || speedAnomaly) {
            if (loitering) meterRegistry.counter("detections", "type", "loitering").increment();
            if (speedAnomaly) meterRegistry.counter("detections", "type", "speed_anomaly").increment();

            log.warn("Detection fired mmsi={} loitering={} speedAnomaly={}", mmsi, loitering, speedAnomaly);
            ctx.forward(record.withValue(withFlags(event, loitering, false, speedAnomaly)));
        }
        // No forward() call for clean records — the topology's downstream filter
        // drops null, but here we simply don't forward, which is cleaner.
    }

    @Override
    public void close() {
        // Nothing to release — the store is managed by Kafka Streams.
    }

    /**
     * Loitering: vessel has been below {@link #LOITER_SPEED_KN} for at least
     * {@link #LOITER_DURATION_MINUTES} minutes.
     *
     * <p>We compare the <em>previous</em> report's speed (stored in {@code prev})
     * with the current one. Both must be below the threshold, and the wall-clock
     * gap between them must exceed the dwell duration. Using two consecutive
     * sub-threshold reports (rather than just the current one) prevents a momentary
     * slow-down from triggering a false positive.
     */
    private boolean detectLoitering(VesselState prev, EnrichedVesselEvent event) {
        if (prev == null) return false;

        double speed = event.getVesselEvent().getSpeed();
        if (speed >= LOITER_SPEED_KN) return false;

        long nowMs = event.getVesselEvent().getTimestamp().toEpochMilli();
        long gapMin = (nowMs - prev.getLastSeenMs()) / 60_000L;

        return prev.getSpeed() < LOITER_SPEED_KN && gapMin >= LOITER_DURATION_MINUTES;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Speed anomaly: Haversine-implied speed deviates from reported SOG by more than
     * {@link #SPEED_ANOMALY_THRESHOLD_KN} knots.
     *
     * <p>A vessel reporting SOG = 5 kn while its position jumps 100 NM in 10 minutes
     * implies ~600 kn — a clear impossibility. Large divergences suggest AIS spoofing
     * (the vessel is broadcasting a false position) or a sensor fault.
     */
    private boolean detectSpeedAnomaly(VesselState prev, EnrichedVesselEvent event) {
        if (prev == null) return false;

        long nowMs = event.getVesselEvent().getTimestamp().toEpochMilli();
        long elapsedMs = nowMs - prev.getLastSeenMs();
        if (elapsedMs <= 0) return false;

        double distMeters = GeoUtils.calculateDistanceInMeters(
                prev.getLatitude(), prev.getLongitude(),
                event.getVesselEvent().getLatitude(),
                event.getVesselEvent().getLongitude()
        );

        double impliedKnots = (distMeters * METERS_TO_NM) / (elapsedMs / 3_600_000.0);
        double reportedKnots = event.getVesselEvent().getSpeed();
        double divergence = Math.abs(impliedKnots - reportedKnots);

        if (divergence > SPEED_ANOMALY_THRESHOLD_KN) {
            log.debug("Speed anomaly mmsi={} implied={} kn reported={} kn diff={}",
                    event.getVesselEvent().getMmsi(),
                    String.format("%.1f", impliedKnots),
                    String.format("%.1f", reportedKnots),
                    String.format("%.1f", divergence));
            return true;
        }
        return false;
    }

    /**
     * Dark vessel: scans the full state store for vessels whose {@code lastSeenMs}
     * is older than {@link #DARK_VESSEL_MINUTES} minutes ago.
     *
     * <p>This fires on a wall-clock schedule (every minute) rather than on incoming
     * records. Stream time only advances when records arrive — it would freeze during
     * the very silence we need to detect. Wall-clock time is the correct choice here.
     *
     * <p>Transitions are edge-triggered: a vessel is flagged on the dark→silent
     * crossing only, not on every subsequent scan while it remains silent. When a
     * vessel comes back online (a new record arrives and its {@code lastSeenMs} is
     * updated), the flag is cleared on the next scan.
     */
    private void checkDarkVessels(long wallClockMs) {
        long thresholdMs = wallClockMs - (DARK_VESSEL_MINUTES * 60_000L);

        try (KeyValueIterator<String, VesselState> it = store.all()) {
            while (it.hasNext()) {
                var entry = it.next();
                VesselState state = entry.value;
                if (state == null) continue;

                boolean wasDark = state.isDarkVessel();
                boolean isDark = state.getLastSeenMs() < thresholdMs;

                if (isDark && !wasDark) {
                    state.setDarkVessel(true);
                    store.put(entry.key, state);
                    meterRegistry.counter("detections", "type", "dark_vessel").increment();
                    log.warn("Dark vessel detected mmsi={} lastSeen={}",
                            state.getMmsi(), Instant.ofEpochMilli(state.getLastSeenMs()));

                } else if (!isDark && wasDark) {
                    state.setDarkVessel(false);
                    store.put(entry.key, state);
                    log.info("Dark vessel cleared mmsi={} (back online)", state.getMmsi());
                }
            }
        }
    }
}
