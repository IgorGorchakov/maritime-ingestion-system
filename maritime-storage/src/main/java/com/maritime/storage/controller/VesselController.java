package com.maritime.storage.controller;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.maritime.common.dto.EnrichedVesselEvent;
import com.maritime.common.dto.VesselEvent;
import com.maritime.common.serde.AvroJson;
import com.maritime.storage.service.AwsStorageService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/v1")
public class VesselController {

    private final AwsStorageService awsStorageService;

    @Autowired
    public VesselController(AwsStorageService awsStorageService) {
        this.awsStorageService = awsStorageService;
    }

    @KafkaListener(topics = "maritime.enriched", groupId = "storage-service")
    public void consumeEnrichedEvent(EnrichedVesselEvent event) {
        try {
            VesselEvent vessel = event.getVesselEvent();
            // Avro-JSON: the schema embedded in the record stays the source of truth
            // on the cold tier (Phase 7 switches this S3 format to Parquet).
            String json = AvroJson.toJson(event);
            String key = vessel.getMmsi() + "/" + vessel.getTimestamp().toEpochMilli();

            // Save to "cold" storage (S3)
            awsStorageService.saveToS3("vessel-events/" + key + ".json", json);

            // Save to "hot" storage (DynamoDB) - latest state per vessel
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("mmsi", new AttributeValue().withS(vessel.getMmsi()));
            item.put("riskLevel", new AttributeValue().withS(event.getRiskLevel()));
            item.put("riskScore", new AttributeValue().withN(String.valueOf(event.getRiskScore())));
            item.put("inRestrictedZone", new AttributeValue().withBOOL(event.getInRestrictedZone()));
            if (event.getZoneName() != null) {
                // zoneName is a nullable Avro union; DynamoDB rejects a null S value.
                item.put("zoneName", new AttributeValue().withS(event.getZoneName()));
            }
            item.put("payload", new AttributeValue().withS(json));
            awsStorageService.saveToDynamoDB(item);

        } catch (Exception e) {
            log.error("Failed to persist enriched event for MMSI {}: {}",
                    event.getVesselEvent().getMmsi(), e.getMessage(), e);
        }
    }

    // Returns Avro-JSON as the response body (not a Jackson-bound object): Jackson
    // cannot round-trip an Avro SpecificRecord, so the HTTP hop to the gateway speaks
    // the same Avro-JSON contract the cold tier uses.
    @GetMapping(value = "/vessels/{mmsi}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> getVesselRisk(@PathVariable String mmsi) {
        Map<String, AttributeValue> item = awsStorageService.getVesselRisk(mmsi);
        if (item == null) {
            return ResponseEntity.notFound().build();
        }

        // Prefer the full stored payload (already Avro-JSON) if present.
        if (item.containsKey("payload")) {
            return ResponseEntity.ok(item.get("payload").getS());
        }

        // Fallback: rebuild from flat columns. Avro requires all non-null fields, so the
        // placeholder vessel gets zeroed position + an epoch timestamp (no payload row
        // should normally hit this path).
        VesselEvent placeholder = VesselEvent.newBuilder()
                .setMmsi(mmsi)
                .setLatitude(0.0)
                .setLongitude(0.0)
                .setSpeed(0.0)
                .setHeading(0.0)
                .setTimestamp(java.time.Instant.EPOCH)
                .setEventType("UNKNOWN")
                .build();
        EnrichedVesselEvent event = EnrichedVesselEvent.newBuilder()
                .setVesselEvent(placeholder)
                .setRiskLevel(attr(item, "riskLevel"))
                .setRiskScore(item.containsKey("riskScore") ? Double.parseDouble(item.get("riskScore").getN()) : 0.0)
                .setInRestrictedZone(item.containsKey("inRestrictedZone") && item.get("inRestrictedZone").getBOOL())
                .setZoneName(attr(item, "zoneName"))
                .setDistanceToPort(0.0)
                .build();
        return ResponseEntity.ok(AvroJson.toJson(event));
    }

    private static String attr(Map<String, AttributeValue> item, String name) {
        return item.containsKey(name) ? item.get(name).getS() : null;
    }
}
