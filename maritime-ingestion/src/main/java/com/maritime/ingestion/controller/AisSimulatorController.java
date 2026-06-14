package com.maritime.ingestion.controller;

import com.maritime.common.dto.VesselEvent;
import com.maritime.common.kafka.Topics;
import com.maritime.common.observability.CorrelationIds;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * AIS simulator with realistic vessel tracks designed to exercise the Phase 6 detectors.
 *
 * <h3>Vessel fleet</h3>
 * <table>
 *   <tr><th>MMSI</th><th>Behaviour</th><th>Detector target</th></tr>
 *   <tr><td>123456789</td><td>Normal transit across the Gulf of Mexico</td><td>—</td></tr>
 *   <tr><td>234567890</td><td>Deliberate loiterer — drifts at 0.3 kn in a tight box</td>
 *       <td>loitering</td></tr>
 *   <tr><td>345678901</td><td>Transits normally, then stops transmitting after 12 reports
 *       (simulates transponder off)</td><td>dark vessel</td></tr>
 *   <tr><td>456789012</td><td>Reports speed=2 kn but moves 40 nm between ticks</td>
 *       <td>speed anomaly</td></tr>
 * </table>
 *
 * All vessels start in the Gulf of Mexico simulator area (-90..-80 lon, 29..32 lat)
 * so they overlap the seeded PostGIS zones (EEZ + Port of New Orleans).
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/simulate")
public class AisSimulatorController {

    // ── Fixed MMSIs for reproducible demo scenarios ──────────────────────────
    private static final String MMSI_NORMAL        = "123456789";
    private static final String MMSI_LOITERER      = "234567890";
    private static final String MMSI_DARK_VESSEL   = "345678901";
    private static final String MMSI_SPEED_ANOMALY = "456789012";

    /** After this many ticks the dark-vessel stops transmitting. */
    private static final int DARK_VESSEL_SILENCE_AFTER = 12;

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Counter eventsIngested;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private volatile ScheduledFuture<?> task;

    // Mutable track state — only written from the single scheduler thread.
    private final AtomicInteger tick = new AtomicInteger(0);

    // Normal vessel: waypoints along the Gulf Coast eastbound.
    private static final double[][] NORMAL_WAYPOINTS = {
        {30.5, -90.0}, {30.6, -89.5}, {30.7, -89.0}, {30.8, -88.5},
        {30.9, -88.0}, {31.0, -87.5}, {31.1, -87.0}, {31.2, -86.5},
        {31.3, -86.0}, {31.4, -85.5}, {31.5, -85.0}, {31.6, -84.5}
    };

    @Autowired
    public AisSimulatorController(KafkaTemplate<String, Object> kafkaTemplate,
                                  MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.eventsIngested = Counter.builder("events.ingested")
                .description("AIS events produced to the raw topic")
                .register(meterRegistry);
    }

    @PostMapping("/start")
    public String startSimulation() {
        if (isRunning.compareAndSet(false, true)) {
            tick.set(0);
            task = scheduler.scheduleAtFixedRate(this::generateAndSendEvents, 0, 2, TimeUnit.SECONDS);
            log.info("Simulation started — {} vessels in fleet", 4);
            return "Simulation started (4-vessel fleet: normal, loiterer, dark-vessel, speed-anomaly)";
        }
        return "Simulation is already running";
    }

    @PostMapping("/stop")
    public String stopSimulation() {
        if (isRunning.compareAndSet(true, false)) {
            if (task != null) task.cancel(false);
            return "Simulation stopped";
        }
        return "Simulation is not running";
    }

    private void generateAndSendEvents() {
        int t = tick.getAndIncrement();
        Instant now = Instant.now();

        emit(buildNormalVessel(t, now));
        emit(buildLoiterer(t, now));
        if (t < DARK_VESSEL_SILENCE_AFTER) {
            emit(buildDarkVessel(t, now));
        }
        // After tick DARK_VESSEL_SILENCE_AFTER the dark vessel stops emitting,
        // so the punctuator in MaritimeTopology will flag it after 10 minutes.
        emit(buildSpeedAnomalyVessel(t, now));
    }

    // ── Vessel builders ──────────────────────────────────────────────────────

    /** Steady eastbound transit; speed ~12 kn; crosses the RESTRICTED zone around tick 4–5. */
    private VesselEvent buildNormalVessel(int t, Instant now) {
        int wp = t % NORMAL_WAYPOINTS.length;
        return VesselEvent.newBuilder()
                .setMmsi(MMSI_NORMAL)
                .setLatitude(NORMAL_WAYPOINTS[wp][0])
                .setLongitude(NORMAL_WAYPOINTS[wp][1])
                .setSpeed(12.0)
                .setHeading(90.0)   // heading east
                .setTimestamp(now)
                .setEventType("AIS")
                .build();
    }

    /**
     * Loiterer: stays near (-89.8, 30.1) with tiny random drift and very low speed.
     * After LOITER_DURATION_MINUTES (5 min) of consecutive sub-1-kn reports, the
     * topology will fire the loitering detector.
     */
    private VesselEvent buildLoiterer(int t, Instant now) {
        // Small random drift within ~0.05° so position changes slightly each tick
        // (a completely static position would look like a spoofed/frozen feed).
        double lat = 30.1 + (Math.random() - 0.5) * 0.05;
        double lon = -89.8 + (Math.random() - 0.5) * 0.05;
        return VesselEvent.newBuilder()
                .setMmsi(MMSI_LOITERER)
                .setLatitude(lat)
                .setLongitude(lon)
                .setSpeed(0.3)       // well below LOITER_SPEED_KN = 1.0
                .setHeading(0.0)
                .setTimestamp(now)
                .setEventType("AIS")
                .build();
    }

    /**
     * Dark vessel: moves normally until tick DARK_VESSEL_SILENCE_AFTER, then
     * the simulator stops emitting. The topology's wall-clock punctuator will
     * flag it as dark after 10 minutes of silence.
     */
    private VesselEvent buildDarkVessel(int t, Instant now) {
        double lat = 30.0 + t * 0.05;
        double lon = -89.5 + t * 0.1;
        return VesselEvent.newBuilder()
                .setMmsi(MMSI_DARK_VESSEL)
                .setLatitude(lat)
                .setLongitude(lon)
                .setSpeed(8.0)
                .setHeading(45.0)
                .setTimestamp(now)
                .setEventType("AIS")
                .build();
    }

    /**
     * Speed-anomaly vessel: reports SOG = 2 kn but jumps ~0.4° (~24 nm) per 2-second
     * tick, implying ~43 200 kn. The topology computes Haversine-implied speed and
     * flags the divergence immediately on the second report.
     */
    private VesselEvent buildSpeedAnomalyVessel(int t, Instant now) {
        // Large position jump each tick to create an obvious implied-speed anomaly.
        double lat = 29.5 + (t % 10) * 0.4;
        double lon = -90.0 + (t % 6) * 0.4;
        return VesselEvent.newBuilder()
                .setMmsi(MMSI_SPEED_ANOMALY)
                .setLatitude(lat)
                .setLongitude(lon)
                .setSpeed(2.0)       // reported SOG is suspiciously low given the jump
                .setHeading(180.0)
                .setTimestamp(now)
                .setEventType("AIS")
                .build();
    }

    // ── Kafka publish ────────────────────────────────────────────────────────

    private void emit(VesselEvent event) {
        String correlationId = CorrelationIds.newId();
        MDC.put(CorrelationIds.MDC_KEY, correlationId);
        try {
            kafkaTemplate.send(Topics.AIS_RAW, event.getMmsi(), event).whenComplete((result, ex) -> {
                MDC.put(CorrelationIds.MDC_KEY, correlationId);
                try {
                    if (ex == null) {
                        eventsIngested.increment();
                        log.debug("Sent event MMSI={} speed={} lat={} lon={}",
                                event.getMmsi(), event.getSpeed(),
                                event.getLatitude(), event.getLongitude());
                    } else {
                        log.error("Failed to send event for MMSI {}: {}",
                                event.getMmsi(), ex.getMessage(), ex);
                    }
                } finally {
                    MDC.remove(CorrelationIds.MDC_KEY);
                }
            });
        } finally {
            MDC.remove(CorrelationIds.MDC_KEY);
        }
    }
}
