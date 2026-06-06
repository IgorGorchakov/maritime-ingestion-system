package com.maritime.gateway.controller;

import com.maritime.common.dto.EnrichedVesselEvent;
import com.maritime.common.serde.AvroJson;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

@RestController
@RequestMapping("/api/v1/intelligence")
public class GatewayController {

    private final RestTemplate restTemplate;
    private final String storageServiceUrl;

    public GatewayController(
            @Value("${maritime.storage.service.url:http://localhost:8083}") String storageServiceUrl) {
        this.restTemplate = new RestTemplate();
        this.storageServiceUrl = storageServiceUrl;
    }

    @GetMapping("/{mmsi}")
    public ResponseEntity<EnrichedVesselEvent> getVesselIntelligence(@PathVariable String mmsi) {
        // Storage returns Avro-JSON; fetch it as a raw String and decode with the
        // shared Avro codec. Jackson can't round-trip an Avro SpecificRecord, so we
        // deliberately avoid binding straight to EnrichedVesselEvent.class here.
        String json = restTemplate.getForObject(
                storageServiceUrl + "/api/v1/vessels/" + mmsi,
                String.class
        );
        if (json == null || json.isBlank()) {
            return ResponseEntity.notFound().build();
        }
        EnrichedVesselEvent vesselData = AvroJson.fromJson(json, EnrichedVesselEvent.class);
        return ResponseEntity.ok(vesselData);
    }
}