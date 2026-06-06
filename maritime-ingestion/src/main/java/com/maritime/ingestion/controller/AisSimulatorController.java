package com.maritime.ingestion.controller;

import com.maritime.common.dto.VesselEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@RestController
@RequestMapping("/api/v1/simulate")
public class AisSimulatorController {

    private static final String TOPIC = "maritime.ais.raw";
    private final KafkaTemplate<String, VesselEvent> kafkaTemplate;
    // One long-lived executor for the bean's lifetime. We cancel the scheduled task on
    // stop rather than shutting the executor down, so start/stop/start works (a
    // shut-down executor can never be reused -> RejectedExecutionException).
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private volatile ScheduledFuture<?> task;

    @Autowired
    public AisSimulatorController(KafkaTemplate<String, VesselEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
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
        VesselEvent event = VesselEvent.newBuilder()
                .setMmsi(String.valueOf((int) (Math.random() * 90000000) + 10000000))
                .setLatitude(30.0 + Math.random() * 10.0) // Random lat between 30 and 40
                .setLongitude(-90.0 + Math.random() * 10.0) // Random lon between -90 and -80
                .setSpeed(5.0 + Math.random() * 15.0)
                .setHeading(Math.random() * 360)
                .setTimestamp(java.time.Instant.now())
                .setEventType("AIS")
                .build();

        // Use MMSI as key for partitioning
        kafkaTemplate.send(TOPIC, event.getMmsi(), event).whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Sent event: " + event.getMmsi());
            } else {
                System.err.println("Failed to send event: " + ex.getMessage());
            }
        });
    }
}