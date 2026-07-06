package com.maritime.ingestion.controller;

import com.maritime.ingestion.service.AisSimulatorService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST entry point for controlling the AIS simulation.
 *
 * <p>This is a thin HTTP adapter: it exposes {@code /start} and {@code /stop} and
 * formats the response, delegating all behaviour to {@link AisSimulatorService}. The
 * simulation engine — scheduling, vessel motion models, and Kafka publishing — lives in
 * the service, so this class has a single reason to change (the HTTP contract) and the
 * engine can be driven from other entry points or tested without Spring MVC.
 */
@RestController
@RequestMapping("/api/v1/simulate")
public class AisSimulatorController {

    private final AisSimulatorService simulator;

    public AisSimulatorController(AisSimulatorService simulator) {
        this.simulator = simulator;
    }

    @PostMapping("/start")
    public String startSimulation() {
        if (simulator.start()) {
            return String.format("Simulation started (%d-vessel fleet)", simulator.fleetSize());
        }
        return "Simulation is already running";
    }

    @PostMapping("/stop")
    public String stopSimulation() {
        return simulator.stop() ? "Simulation stopped" : "Simulation is not running";
    }
}
