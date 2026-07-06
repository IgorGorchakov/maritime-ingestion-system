package com.maritime.storage.controller;

import com.maritime.common.serde.AvroJson;
import com.maritime.storage.service.VesselStateHotStore;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Real-time vessel-state query API. Serves the latest per-MMSI state from the Postgres
 * hot tier as canonical Avro-JSON.
 *
 * <p>HTTP only. The Kafka consume-and-persist path lives in
 * {@link com.maritime.storage.service.VesselEventConsumer}; the two were split (audit H2)
 * so the REST endpoint and the Kafka listeners — which evolve independently (caching and
 * rate-limiting here; ack mode, retry, and dead-letter routing there) — no longer share a
 * class. Depends only on the {@link VesselStateHotStore} port, never the concrete store.
 */
@RestController
@RequestMapping("/api/v1")
public class VesselQueryController {

    private final VesselStateHotStore hotTier;

    public VesselQueryController(VesselStateHotStore hotTier) {
        this.hotTier = hotTier;
    }

    @GetMapping(value = "/vessels/{mmsi}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> getVesselRisk(@PathVariable String mmsi) {
        return hotTier.findByMmsi(mmsi)
                .map(AvroJson::toJson)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
