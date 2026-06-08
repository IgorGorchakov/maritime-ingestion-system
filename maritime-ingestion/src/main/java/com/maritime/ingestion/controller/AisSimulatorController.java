package com.maritime.ingestion.controller;

import com.maritime.common.dto.VesselEvent;
import com.maritime.common.observability.CorrelationIds;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@RestController
@RequestMapping("/api/v1/simulate")
public class AisSimulatorController {

    private static final String TOPIC = "maritime.ais.raw";
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Counter eventsIngested;
    // One long-lived executor for the bean's lifetime. We cancel the scheduled task on
    // stop rather than shutting the executor down, so start/stop/start works (a
    // shut-down executor can never be reused -> RejectedExecutionException).
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private volatile ScheduledFuture<?> task;

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
            task = scheduler.scheduleAtFixedRate(this::generateAndSendEvent, 0, 1, TimeUnit.SECONDS);
            return "Simulation started";
        }
        return "Simulation is already running";
    }

    @PostMapping("/stop")
    public String stopSimulation() {
        if (isRunning.compareAndSet(true, false)) {
            if (task != null) {
                task.cancel(false);
            }
            return "Simulation stopped";
        }
        return "Simulation is not running";
    }

    private void generateAndSendEvent() {
        // This runs on a scheduler thread with no inbound request, so there is no
        // correlation id in MDC yet. Mint one per event so the producer interceptor
        // stamps a real id onto the record (rather than minting an orphan) and the
        // event is greppable from here through streaming to storage.
        String correlationId = CorrelationIds.newId();
        MDC.put(CorrelationIds.MDC_KEY, correlationId);
        try {
            VesselEvent event = VesselEvent.newBuilder()
                    // MMSI is exactly 9 digits (ITU standard); generate in [100000000, 999999999].
                    .setMmsi(String.valueOf((long) (Math.random() * 900000000L) + 100000000L))
                    .setLatitude(30.0 + Math.random() * 10.0) // Random lat between 30 and 40
                    .setLongitude(-90.0 + Math.random() * 10.0) // Random lon between -90 and -80
                    .setSpeed(5.0 + Math.random() * 15.0)
                    .setHeading(Math.random() * 360)
                    .setTimestamp(java.time.Instant.now())
                    .setEventType("AIS")
                    .build();

            // Use MMSI as key for partitioning.
            // The callback runs on the Kafka producer thread, where this thread's MDC
            // is not visible, so re-bind the correlation id there to keep the log line
            // greppable alongside the streaming/storage hops (which already carry it via
            // the Kafka header stamped by CorrelationIdProducerInterceptor).
            kafkaTemplate.send(TOPIC, event.getMmsi(), event).whenComplete((result, ex) -> {
                MDC.put(CorrelationIds.MDC_KEY, correlationId);
                try {
                    if (ex == null) {
                        eventsIngested.increment();
                        log.info("Sent event: {}", event.getMmsi());
                    } else {
                        log.error("Failed to send event: {}", ex.getMessage(), ex);
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