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
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Consumes enriched vessel events from Kafka and persists them to:
 * <ul>
 *   <li><b>S3 (cold tier, Parquet):</b> one file per event, partitioned by
 *       {@code date=/mmsi=} for Spark partition pruning (Phase 7).</li>
 *   <li><b>DynamoDB (hot tier):</b> latest state per MMSI for real-time REST queries.</li>
 * </ul>
 */
@Slf4j
@RestController
@RequestMapping("/api/v1")
public class VesselController {

    private final AwsStorageService awsStorageService;

    @Autowired
    public VesselController(AwsStorageService awsStorageService) {
        this.awsStorageService = awsStorageService;
    }

    /**
     * Consumes base enriched events (zone + risk, no detection flags).
     * This is the primary cold + hot tier write path for every vessel event.
     */
    @KafkaListener(topics = "maritime.enriched", groupId = "storage-service")
    public void consumeEnrichedEvent(EnrichedVesselEvent event, Acknowledgment ack) {
        persist(event, ack);
    }

    /**
     * Consumes detection events from the topology's dedicated output topic.
     *
     * maritime.detections receives only events where at least one flag fired
     * (loitering / darkVessel / speedAnomaly). Subscribing to a separate topic
     * instead of maritime.enriched means the topology's output can never feed
     * back into its own input — C1 processing loop structurally impossible.
     *
     * We upsert into the same DynamoDB hot tier so a GET /vessels/{mmsi}
     * immediately reflects the latest detection state.
     */
    @KafkaListener(topics = "maritime.detections", groupId = "storage-service")
    public void consumeDetectionEvent(EnrichedVesselEvent event, Acknowledgment ack) {
        persist(event, ack);
    }

    /**
     * Shared persistence logic for both listeners.
     * Writes to S3 Parquet cold tier and DynamoDB hot tier, then acks.
     * Any exception propagates to DefaultErrorHandler (retry → DLT).
     */
    private void persist(EnrichedVesselEvent event, Acknowledgment ack) {
        VesselEvent vessel = event.getVesselEvent();

        // ── Cold tier (S3, Parquet) ───────────────────────────────────────────
        // Phase 7: switched from Avro-JSON to Parquet (columnar + Snappy compression).
        // Key layout: vessel-events/date=<yyyy-MM-dd>/mmsi=<mmsi>/<epochMs>.parquet
        // Spark reads these as a partitioned Dataset with partition pruning on date.
        awsStorageService.saveParquetToS3(event);

        // ── Hot tier (DynamoDB) ───────────────────────────────────────────────
        // DynamoDB stores the latest state per vessel for real-time GET /vessels/{mmsi}.
        // We still store Avro-JSON in the payload column for the gateway response;
        // this keeps the HTTP contract unchanged while the cold tier moves to Parquet.
        String json = AvroJson.toJson(event);
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("mmsi",           new AttributeValue().withS(vessel.getMmsi()));
        item.put("riskLevel",      new AttributeValue().withS(event.getRiskLevel()));
        item.put("riskScore",      new AttributeValue().withN(String.valueOf(event.getRiskScore())));
        item.put("inRestrictedZone", new AttributeValue().withBOOL(event.getInRestrictedZone()));
        item.put("loitering",      new AttributeValue().withBOOL(event.getLoitering()));
        item.put("darkVessel",     new AttributeValue().withBOOL(event.getDarkVessel()));
        item.put("speedAnomaly",   new AttributeValue().withBOOL(event.getSpeedAnomaly()));
        if (event.getZoneName() != null) {
            item.put("zoneName",   new AttributeValue().withS(event.getZoneName()));
        }
        if (event.getZoneType() != null) {
            item.put("zoneType",   new AttributeValue().withS(event.getZoneType()));
        }
        item.put("payload",        new AttributeValue().withS(json));
        awsStorageService.saveToDynamoDB(item);

        // Offset committed only after both writes succeed.
        ack.acknowledge();
        log.info("Persisted MMSI={} riskLevel={} loitering={} dark={} speedAnomaly={}",
                vessel.getMmsi(), event.getRiskLevel(),
                event.getLoitering(), event.getDarkVessel(), event.getSpeedAnomaly());
    }

    @GetMapping(value = "/vessels/{mmsi}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> getVesselRisk(@PathVariable String mmsi) {
        Map<String, AttributeValue> item = awsStorageService.getVesselRisk(mmsi);
        if (item == null || item.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        if (item.containsKey("payload")) {
            return ResponseEntity.ok(item.get("payload").getS());
        }

        // Fallback: rebuild from flat columns (should not normally be reached).
        VesselEvent placeholder = VesselEvent.newBuilder()
                .setMmsi(mmsi).setLatitude(0.0).setLongitude(0.0)
                .setSpeed(0.0).setHeading(0.0)
                .setTimestamp(java.time.Instant.EPOCH).setEventType("UNKNOWN")
                .build();
        EnrichedVesselEvent event = EnrichedVesselEvent.newBuilder()
                .setVesselEvent(placeholder)
                .setRiskLevel(attr(item, "riskLevel"))
                .setRiskScore(item.containsKey("riskScore")
                        ? Double.parseDouble(item.get("riskScore").getN()) : 0.0)
                .setInRestrictedZone(item.containsKey("inRestrictedZone")
                        && item.get("inRestrictedZone").getBOOL())
                .setZoneName(attr(item, "zoneName"))
                .setZoneType(attr(item, "zoneType"))
                .setDistanceToPort(0.0)
                .setLoitering(item.containsKey("loitering")
                        && item.get("loitering").getBOOL())
                .setDarkVessel(item.containsKey("darkVessel")
                        && item.get("darkVessel").getBOOL())
                .setSpeedAnomaly(item.containsKey("speedAnomaly")
                        && item.get("speedAnomaly").getBOOL())
                .build();
        return ResponseEntity.ok(AvroJson.toJson(event));
    }

    private static String attr(Map<String, AttributeValue> item, String name) {
        return item.containsKey(name) ? item.get(name).getS() : null;
    }
}
