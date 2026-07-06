package com.maritime.api.controller;

import com.maritime.api.dto.VesselDailySummary;
import com.maritime.api.service.VesselIntelligenceService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Public API for vessel intelligence — the serving layer of the Lambda architecture.
 *
 * <p>This class is a thin HTTP adapter: routing, path variables, response codes, and
 * content negotiation only. All data access and orchestration live in
 * {@link VesselIntelligenceService}, which reads the real-time hot tier (speed layer)
 * and the Spark batch rollup (batch layer). Both views key on MMSI, so a client can
 * merge them for the full historical + real-time picture.
 *
 * <h3>Endpoints</h3>
 * <ul>
 *   <li>{@code GET /api/v1/intelligence/{mmsi}} — latest real-time enriched event.</li>
 *   <li>{@code GET /api/v1/intelligence/{mmsi}/history} — daily aggregates and risk
 *       percentiles, most recent first, capped at 90 days. 404 when no batch data
 *       exists yet; the real-time endpoint remains available regardless.</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/v1/intelligence")
public class ApiController {

    private final VesselIntelligenceService service;

    public ApiController(VesselIntelligenceService service) {
        this.service = service;
    }

    @GetMapping(value = "/{mmsi}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> getVesselIntelligence(@PathVariable String mmsi) {
        return service.findRealTime(mmsi)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping(value = "/{mmsi}/history", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<VesselDailySummary>> getVesselHistory(@PathVariable String mmsi) {
        List<VesselDailySummary> history = service.findHistory(mmsi);
        return history.isEmpty()
                ? ResponseEntity.notFound().build()
                : ResponseEntity.ok(history);
    }
}
