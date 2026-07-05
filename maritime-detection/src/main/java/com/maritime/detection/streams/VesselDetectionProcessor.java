package com.maritime.detection.streams;

import com.maritime.common.dto.EnrichedVesselEvent;
import com.maritime.common.dto.VesselEvent;
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
 *       at least {@link #LOITER_DURATION_MINUTES} consecutive minutes.</li>
 *   <li><b>Speed anomaly</b> — Haversine-implied speed diverges from reported SOG
 *       by more than {@link #SPEED_ANOMALY_THRESHOLD_KN} knots.</li>
 *   <li><b>Dark vessel</b> — no AIS report received for {@link #DARK_VESSEL_MINUTES}
 *       minutes, detected by a wall-clock punctuator that scans the full state store.</li>
 * </ol>
 *
 * <h3>Output contract</h3>
 * Records are forwarded downstream only when at least one flag fires. Clean records
 * produce no output — the topology's downstream {@code .filter()} drops nulls, but
 * simply not calling {@code ctx.forward()} is cleaner and equivalent.
 *
 * <h3>Thread safety</h3>
 * Kafka Streams guarantees a single processor instance is accessed by exactly one
 * stream thread at a time. No additional synchronisation is needed.
 */
@Slf4j
public class VesselDetectionProcessor
        implements Processor<String, EnrichedVesselEvent, String, EnrichedVesselEvent> {

    // ── Tuning constants ──────────────────────────────────────────────────────
    static final double LOITER_SPEED_KN            = 1.0;
    static final long   LOITER_DURATION_MINUTES    = 5L;
    static final long   DARK_VESSEL_MINUTES        = 10L;
    static final double SPEED_ANOMALY_THRESHOLD_KN = 15.0;

    private static final double METERS_TO_NM = 1.0 / 1852.0;

    private final MeterRegistry meterRegistry;

    private KeyValueStore<String, VesselState>          store;
    private ProcessorContext<String, EnrichedVesselEvent> ctx;

    public VesselDetectionProcessor(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @Override
    public void init(ProcessorContext<String, EnrichedVesselEvent> context) {
        this.ctx   = context;
        this.store = context.getStateStore(MaritimeTopology.STATE_STORE);
        context.schedule(Duration.ofMinutes(1), PunctuationType.WALL_CLOCK_TIME, this::checkDarkVessels);
        log.info("VesselDetectionProcessor initialised, store={}", MaritimeTopology.STATE_STORE);
    }

    @Override
    public void process(Record<String, EnrichedVesselEvent> record) {
        EnrichedVesselEvent event = record.value();
        if (event == null) return;

        String      mmsi = event.getVesselEvent().getMmsi();
        VesselState prev = store.get(mmsi);

        boolean loitering    = detectLoitering(prev, event);
        boolean speedAnomaly = detectSpeedAnomaly(prev, event);

        // Always update state so lastSeenMs stays current for the dark-vessel punctuator.
        store.put(mmsi, updatedState(mmsi, event, loitering, speedAnomaly));

        if (loitering || speedAnomaly) {
            if (loitering)    meterRegistry.counter("detections", "type", "loitering").increment();
            if (speedAnomaly) meterRegistry.counter("detections", "type", "speed_anomaly").increment();
            log.warn("Detection fired mmsi={} loitering={} speedAnomaly={}", mmsi, loitering, speedAnomaly);
            ctx.forward(record.withValue(withFlags(event, loitering, false, speedAnomaly)));
        }
    }

    @Override
    public void close() {
        // State store lifecycle is managed by Kafka Streams — nothing to release here.
    }

    // ── Detectors ─────────────────────────────────────────────────────────────

    /**
     * Loitering: both the previous and current report are below {@link #LOITER_SPEED_KN}
     * and the elapsed wall-clock gap between them exceeds {@link #LOITER_DURATION_MINUTES}.
     * Requiring two consecutive sub-threshold reports prevents a momentary slow-down
     * from triggering a false positive.
     */
    private boolean detectLoitering(VesselState prev, EnrichedVesselEvent event) {
        if (prev == null) return false;
        double speed = event.getVesselEvent().getSpeed();
        if (speed >= LOITER_SPEED_KN) return false;
        long gapMin = (event.getVesselEvent().getTimestamp().toEpochMilli() - prev.getLastSeenMs()) / 60_000L;
        return prev.getSpeed() < LOITER_SPEED_KN && gapMin >= LOITER_DURATION_MINUTES;
    }

    /**
     * Speed anomaly: Haversine-implied speed (distance / elapsed time) deviates from
     * reported SOG by more than {@link #SPEED_ANOMALY_THRESHOLD_KN} knots. A large
     * divergence suggests AIS position spoofing or a sensor fault.
     */
    private boolean detectSpeedAnomaly(VesselState prev, EnrichedVesselEvent event) {
        if (prev == null) return false;
        long elapsedMs = event.getVesselEvent().getTimestamp().toEpochMilli() - prev.getLastSeenMs();
        if (elapsedMs <= 0) return false;

        double distMeters = GeoUtils.calculateDistanceInMeters(
                prev.getLatitude(), prev.getLongitude(),
                event.getVesselEvent().getLatitude(), event.getVesselEvent().getLongitude());
        double impliedKnots  = (distMeters * METERS_TO_NM) / (elapsedMs / 3_600_000.0);
        double reportedKnots = event.getVesselEvent().getSpeed();
        double divergence    = Math.abs(impliedKnots - reportedKnots);

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
     * Dark vessel: wall-clock punctuator that fires every minute and scans the full
     * state store for vessels whose {@code lastSeenMs} is older than
     * {@link #DARK_VESSEL_MINUTES} minutes. Detection is edge-triggered — a vessel
     * is flagged only on the first crossing, not on every subsequent scan.
     */
    private void checkDarkVessels(long wallClockMs) {
        long thresholdMs = wallClockMs - (DARK_VESSEL_MINUTES * 60_000L);
        try (KeyValueIterator<String, VesselState> it = store.all()) {
            while (it.hasNext()) {
                var         entry = it.next();
                VesselState state = entry.value;
                if (state == null) continue;

                boolean wasDark = state.isDarkVessel();
                boolean isDark  = state.getLastSeenMs() < thresholdMs;

                if (isDark && !wasDark) {
                    state.setDarkVessel(true);
                    store.put(entry.key, state);
                    meterRegistry.counter("detections", "type", "dark_vessel").increment();
                    log.warn("Dark vessel detected mmsi={} lastSeen={}", state.getMmsi(),
                            Instant.ofEpochMilli(state.getLastSeenMs()));
                    ctx.forward(new Record<>(entry.key, toEnrichedEvent(state, true), wallClockMs));
                } else if (!isDark && wasDark) {
                    state.setDarkVessel(false);
                    store.put(entry.key, state);
                    log.info("Dark vessel cleared mmsi={} (back online)", state.getMmsi());
                }
            }
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static VesselState updatedState(String mmsi, EnrichedVesselEvent event,
                                             boolean loitering, boolean speedAnomaly) {
        VesselState s = new VesselState(mmsi,
                event.getVesselEvent().getLatitude(),
                event.getVesselEvent().getLongitude(),
                event.getVesselEvent().getSpeed(),
                event.getVesselEvent().getTimestamp().toEpochMilli());
        s.setLoitering(loitering);
        s.setSpeedAnomaly(speedAnomaly);
        // Carry enrichment context so the dark-vessel punctuator can rebuild the event.
        s.setInRestrictedZone(event.getInRestrictedZone());
        s.setZoneName(event.getZoneName());
        s.setZoneType(event.getZoneType());
        s.setDistanceToPort(event.getDistanceToPort());
        s.setRiskScore(event.getRiskScore());
        s.setRiskLevel(event.getRiskLevel());
        return s;
    }

    /** Rebuilds an {@code EnrichedVesselEvent} from retained state, with {@code darkVessel} set. */
    private static EnrichedVesselEvent toEnrichedEvent(VesselState s, boolean dark) {
        VesselEvent v = VesselEvent.newBuilder()
                .setMmsi(s.getMmsi())
                .setLatitude(s.getLatitude())
                .setLongitude(s.getLongitude())
                .setSpeed(s.getSpeed())
                .setHeading(0.0)   // heading is not retained in state; default to 0
                .setTimestamp(Instant.ofEpochMilli(s.getLastSeenMs()))
                .setEventType("AIS")
                .build();
        return EnrichedVesselEvent.newBuilder()
                .setVesselEvent(v)
                .setInRestrictedZone(s.isInRestrictedZone())
                .setZoneName(s.getZoneName())
                .setZoneType(s.getZoneType())
                .setDistanceToPort(s.getDistanceToPort())
                .setRiskScore(s.getRiskScore())
                .setRiskLevel(s.getRiskLevel())
                .setLoitering(s.isLoitering())
                .setDarkVessel(dark)
                .setSpeedAnomaly(s.isSpeedAnomaly())
                .build();
    }

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
}
