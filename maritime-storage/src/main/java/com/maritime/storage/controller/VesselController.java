package com.maritime.storage.controller;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.maritime.common.dto.EnrichedVesselEvent;
import com.maritime.common.dto.VesselEvent;
import com.maritime.storage.service.AwsStorageService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/v1")
public class VesselController {

    private final AwsStorageService awsStorageService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    public VesselController(AwsStorageService awsStorageService) {
        this.awsStorageService = awsStorageService;
    }

    @KafkaListener(topics = "maritime.enriched", groupId = "storage-service")
    public void consumeEnrichedEvent(EnrichedVesselEvent event) {
        try {
            VesselEvent vessel = event.getVesselEvent();
            String json = objectMapper.writeValueAsString(event);
            String key = vessel.getMmsi() + "/" + vessel.getTimestamp().toEpochMilli();

            // Save to "cold" storage (S3)
            awsStorageService.saveToS3("vessel-events/" + key + ".json", json);

            // Save to "hot" storage (DynamoDB) - latest state per vessel
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("mmsi", new AttributeValue().withS(vessel.getMmsi()));
            item.put("riskLevel", new AttributeValue().withS(event.getRiskLevel()));
            item.put("riskScore", new AttributeValue().withN(String.valueOf(event.getRiskScore())));
            item.put("inRestrictedZone", new AttributeValue().withBOOL(event.isInRestrictedZone()));
            item.put("zoneName", new AttributeValue().withS(event.getZoneName()));
            item.put("payload", new AttributeValue().withS(json));
            awsStorageService.saveToDynamoDB(item);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @GetMapping("/vessels/{mmsi}")
    public ResponseEntity<EnrichedVesselEvent> getVesselRisk(@PathVariable String mmsi) {
        Map<String, AttributeValue> item = awsStorageService.getVesselRisk(mmsi);
        if (item == null) {
            return ResponseEntity.notFound().build();
        }

        // Prefer the full stored payload if present; otherwise reconstruct from columns.
        if (item.containsKey("payload")) {
            try {
                EnrichedVesselEvent event =
                        objectMapper.readValue(item.get("payload").getS(), EnrichedVesselEvent.class);
                return ResponseEntity.ok(event);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        EnrichedVesselEvent event = EnrichedVesselEvent.builder()
                .vesselEvent(VesselEvent.builder().mmsi(mmsi).build())
                .riskLevel(attr(item, "riskLevel"))
                .riskScore(item.containsKey("riskScore") ? Double.parseDouble(item.get("riskScore").getN()) : 0.0)
                .inRestrictedZone(item.containsKey("inRestrictedZone") && item.get("inRestrictedZone").getBOOL())
                .zoneName(attr(item, "zoneName"))
                .build();
        return ResponseEntity.ok(event);
    }

    private static String attr(Map<String, AttributeValue> item, String name) {
        return item.containsKey(name) ? item.get(name).getS() : null;
    }
}
