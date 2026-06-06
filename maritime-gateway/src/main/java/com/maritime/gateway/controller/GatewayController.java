package com.maritime.gateway.controller;

import com.maritime.common.dto.EnrichedVesselEvent;
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
        EnrichedVesselEvent vesselData = restTemplate.getForObject(
                storageServiceUrl + "/api/v1/vessels/" + mmsi,
                EnrichedVesselEvent.class
        );
        return ResponseEntity.ok(vesselData);
    }
}