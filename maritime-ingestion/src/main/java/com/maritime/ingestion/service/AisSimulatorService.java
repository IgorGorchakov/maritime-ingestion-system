package com.maritime.ingestion.service;

import com.maritime.common.dto.VesselEvent;
import com.maritime.common.observability.CorrelationIds;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * AIS simulation engine — owns the scheduler and Kafka publishing, and drives the
 * fleet defined in {@link SimulatedVessel}. Each vessel type exercises a different
 * detector:
 *
 * <ul>
 *   <li>{@code LOITERER}      → loitering detector (speed &lt; 1 kn for 5+ min)</li>
 *   <li>{@code DARK_VESSEL}   → dark-vessel punctuator (silent after 12 ticks)</li>
 *   <li>{@code SPEED_ANOMALY} → speed-anomaly detector (implied speed vs. reported)</li>
 * </ul>
 *
 * <p>This is the business layer behind
 * {@link com.maritime.ingestion.controller.AisSimulatorController}: the controller only
 * starts and stops the simulation and formats the HTTP response, while all scheduling
 * and event emission live here. Per-vessel motion and transmission behaviour live in
 * {@link SimulatedVessel}, so this class stays a uniform loop with no per-vessel
 * branching. Adding vessels to the fleet requires only editing {@link SimulatedVessel}.
 */
@Slf4j
@Service
public class AisSimulatorService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Counter eventsIngested;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private volatile ScheduledFuture<?> task;
    private final AtomicInteger tick = new AtomicInteger(0);

    public AisSimulatorService(KafkaTemplate<String, Object> kafkaTemplate,
                               MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.eventsIngested = Counter.builder("events.ingested")
                .description("AIS events produced to the raw topic")
                .register(meterRegistry);
    }

    /** Number of vessels in the fleet — exposed so callers can build status messages. */
    public int fleetSize() {
        return SimulatedVessel.values().length;
    }

    /** @return {@code true} if the simulation is currently running. */
    public boolean isRunning() {
        return isRunning.get();
    }

    /**
     * Starts the periodic simulation if it is not already running.
     *
     * @return {@code true} if this call started the simulation, {@code false} if it was
     *         already running.
     */
    public boolean start() {
        if (isRunning.compareAndSet(false, true)) {
            tick.set(0);
            task = scheduler.scheduleAtFixedRate(this::generateAndSendEvents, 0, 2, TimeUnit.SECONDS);
            log.info("Simulation started — {} vessels in fleet", fleetSize());
            return true;
        }
        return false;
    }

    /**
     * Stops the simulation if it is running. Only the scheduled task is cancelled; the
     * underlying thread pool is kept alive so the simulation can be restarted. The pool
     * itself is released in {@link #shutdown()} when the Spring context closes.
     *
     * @return {@code true} if this call stopped a running simulation, {@code false} if it
     *         was not running.
     */
    public boolean stop() {
        if (isRunning.compareAndSet(true, false)) {
            if (task != null) task.cancel(false);
            return true;
        }
        return false;
    }

    /**
     * Shuts the scheduler down when the Spring context closes. Without this the
     * non-daemon threads created by {@link Executors#newScheduledThreadPool(int)} keep
     * the JVM alive after context shutdown, causing containers to hang on stop and the
     * test JVM not to exit.
     */
    @PreDestroy
    public void shutdown() {
        stop();
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    // ── Event generation ─────────────────────────────────────────────────────

    private void generateAndSendEvents() {
        int t = tick.getAndIncrement();
        Instant now = Instant.now();

        for (SimulatedVessel vessel : SimulatedVessel.values()) {
            if (!vessel.transmitsAt(t)) continue;   // e.g. the dark vessel goes silent
            double[] motion = vessel.motionAt(t);   // [latitude, longitude, heading]
            emit(vessel, vesselEvent(vessel, motion[0], motion[1], vessel.speed, motion[2], now));
        }
    }

    // ── Shared factory ───────────────────────────────────────────────────────

    private static VesselEvent vesselEvent(SimulatedVessel vessel, double lat, double lon,
                                            double speed, double heading, Instant now) {
        return VesselEvent.newBuilder()
                .setMmsi(vessel.mmsi)
                .setLatitude(lat)
                .setLongitude(lon)
                .setSpeed(speed)
                .setHeading(heading)
                .setTimestamp(now)
                .setEventType("AIS")
                .setSourceType(vessel.aisSource.name())
                .build();
    }

    // ── Kafka publish ────────────────────────────────────────────────────────

    private void emit(SimulatedVessel vessel, VesselEvent event) {
        String correlationId = CorrelationIds.newId();
        MDC.put(CorrelationIds.MDC_KEY, correlationId);
        try {
            kafkaTemplate.send(vessel.aisSource.topic, event.getMmsi(), event).whenComplete((result, ex) -> {
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
