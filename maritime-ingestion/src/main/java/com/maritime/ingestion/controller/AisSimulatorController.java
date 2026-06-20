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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * AIS simulator — emits realistic vessel tracks for the full fleet defined in
 * {@link SimulatedVessel}. Each vessel type exercises a different detector:
 *
 * <ul>
 *   <li>{@code LOITERER}      → loitering detector (speed &lt; 1 kn for 5+ min)</li>
 *   <li>{@code DARK_VESSEL}   → dark-vessel punctuator (silent after 12 ticks)</li>
 *   <li>{@code SPEED_ANOMALY} → speed-anomaly detector (implied speed vs. reported)</li>
 * </ul>
 *
 * Adding vessels to the fleet requires only editing {@link SimulatedVessel}.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/simulate")
public class AisSimulatorController {

    /** After this many ticks the dark vessel stops transmitting. */
    private static final int DARK_VESSEL_SILENCE_AFTER = 12;

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Counter eventsIngested;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private volatile ScheduledFuture<?> task;
    private final AtomicInteger tick = new AtomicInteger(0);

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
            log.info("Simulation started — {} vessels in fleet", SimulatedVessel.values().length);
            return String.format("Simulation started (%d-vessel fleet)", SimulatedVessel.values().length);
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

    // ── Event generation ─────────────────────────────────────────────────────

    private void generateAndSendEvents() {
        int t = tick.getAndIncrement();
        Instant now = Instant.now();

        for (SimulatedVessel vessel : SimulatedVessel.values()) {
            VesselEvent event = buildEvent(vessel, t, now);
            if (event != null) emit(event);
        }
    }

    private VesselEvent buildEvent(SimulatedVessel vessel, int t, Instant now) {
        return switch (vessel) {
            case LOITERER      -> buildLoiterer(t, now);
            case DARK_VESSEL   -> t < DARK_VESSEL_SILENCE_AFTER ? buildDarkVessel(t, now) : null;
            case SPEED_ANOMALY -> buildSpeedAnomalyVessel(t, now);
            default            -> buildWaypointVessel(vessel, t, now);
        };
    }

    // ── Vessel-specific builders ─────────────────────────────────────────────

    /**
     * Loiterer: smooth circular drift (~0.02° radius) centred near (-89.8, 30.1).
     * Deterministic sin/cos avoids the jitter of Math.random() while still
     * producing visible movement. Speed is well below the 1-kn loitering threshold.
     */
    private VesselEvent buildLoiterer(int t, Instant now) {
        double angle = t * 0.2;   // radians; full circle every ~31 ticks (~62 s)
        double lat = 30.10 + Math.sin(angle) * 0.02;
        double lon = -89.80 + Math.cos(angle) * 0.02;
        // Heading tangent to the circle
        double heading = (Math.toDegrees(angle + Math.PI / 2) + 360) % 360;
        return vesselEvent(SimulatedVessel.LOITERER.mmsi, lat, lon,
                SimulatedVessel.LOITERER.speed, heading, now);
    }

    /**
     * Dark vessel: slow north-east transit; stops transmitting after
     * {@code DARK_VESSEL_SILENCE_AFTER} ticks so the dark-vessel punctuator fires.
     */
    private VesselEvent buildDarkVessel(int t, Instant now) {
        double lat = 30.0 + t * 0.01;
        double lon = -89.5 + t * 0.02;
        return vesselEvent(SimulatedVessel.DARK_VESSEL.mmsi, lat, lon,
                SimulatedVessel.DARK_VESSEL.speed, 45.0, now);
    }

    /**
     * Speed-anomaly vessel: reports SOG = 2 kn but jumps ~0.4° (~24 nm) per
     * 2-second tick. The detector flags the divergence from implied speed on the
     * second report.
     */
    private VesselEvent buildSpeedAnomalyVessel(int t, Instant now) {
        double lat = 29.5 + (t % 10) * 0.4;
        double lon = -90.0 + (t % 6) * 0.4;
        return vesselEvent(SimulatedVessel.SPEED_ANOMALY.mmsi, lat, lon,
                SimulatedVessel.SPEED_ANOMALY.speed, 180.0, now);
    }

    /** Generic waypoint follower — interpolates smoothly between waypoints. */
    private VesselEvent buildWaypointVessel(SimulatedVessel vessel, int t, Instant now) {
        double[] pos     = vessel.positionAt(t);
        double   heading = vessel.headingAt(t);
        return vesselEvent(vessel.mmsi, pos[0], pos[1], vessel.speed, heading, now);
    }

    // ── Shared factory ───────────────────────────────────────────────────────

    private static VesselEvent vesselEvent(String mmsi, double lat, double lon,
                                            double speed, double heading, Instant now) {
        return VesselEvent.newBuilder()
                .setMmsi(mmsi)
                .setLatitude(lat)
                .setLongitude(lon)
                .setSpeed(speed)
                .setHeading(heading)
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
