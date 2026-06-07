package com.maritime.streaming.service;

import com.maritime.common.dto.EnrichedVesselEvent;
import com.maritime.common.dto.VesselEvent;
import com.maritime.common.geo.GeoUtils;
import com.maritime.common.validation.ValidationResult;
import com.maritime.common.validation.VesselEventValidator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;

@Service
public class RiskScorerService {

    private static final String ENRICHED_TOPIC = "maritime.enriched";
    private static final String QUARANTINE_TOPIC = "maritime.ais.quarantine";
    private static final String REASON_HEADER = "reason";

    private final KafkaTemplate<String, EnrichedVesselEvent> kafkaTemplate;
    private final KafkaTemplate<String, VesselEvent> quarantineKafkaTemplate;
    private final VesselEventValidator validator;
    private final DedupService dedupService;
    private final Random random = new Random();

    // Example restricted zone (simple rectangle for demo)
    // First and last coordinates must match to form a closed JTS LinearRing.
    private static final List<double[]> RESTRICTED_ZONE = List.of(
            new double[]{-88.0, 32.0},
            new double[]{-88.0, 35.0},
            new double[]{-85.0, 35.0},
            new double[]{-85.0, 32.0},
            new double[]{-88.0, 32.0}
    );

    @Autowired
    public RiskScorerService(KafkaTemplate<String, EnrichedVesselEvent> kafkaTemplate,
                             KafkaTemplate<String, VesselEvent> quarantineKafkaTemplate,
                             VesselEventValidator validator,
                             DedupService dedupService) {
        this.kafkaTemplate = kafkaTemplate;
        this.quarantineKafkaTemplate = quarantineKafkaTemplate;
        this.validator = validator;
        this.dedupService = dedupService;
    }

    @KafkaListener(topics = "maritime.ais.raw", groupId = "streaming-service")
    public void consumeAndScore(VesselEvent event, Acknowledgment ack) {
        System.out.println("Received event for MMSI: " + event.getMmsi());

        // ---- Validate (E → V) ----
        ValidationResult result = validator.validate(event);
        if (!result.isValid()) {
            quarantine(event, result.getReason());
            ack.acknowledge();
            return;
        }

        // ---- Dedup (V → T guard) ----
        String mmsi = event.getMmsi();
        long timestamp = event.getTimestamp().toEpochMilli();
        if (dedupService.isDuplicate(mmsi, timestamp)) {
            quarantine(event, "duplicate");
            ack.acknowledge();
            return;
        }

        // 1. Check if in restricted zone
        boolean inRestrictedZone = GeoUtils.isPointInPolygon(event.getLatitude(), event.getLongitude(), RESTRICTED_ZONE);

        // 2. Simulate distance to port (random for demo)
        double distanceToPort = random.nextDouble() * 100;

        // 3. Calculate risk score
        double riskScore = 0.0;
        String riskLevel = "LOW";

        if (inRestrictedZone) {
            riskScore += 50;
            riskLevel = "MEDIUM";
        }

        if (distanceToPort < 10) {
            riskScore += 20;
            riskLevel = "HIGH";
        }

        if (event.getSpeed() > 25) {
            riskScore += 10;
        }

        // 4. Build enriched event
        EnrichedVesselEvent enrichedEvent = EnrichedVesselEvent.newBuilder()
                .setVesselEvent(event)
                .setInRestrictedZone(inRestrictedZone)
                .setZoneName(inRestrictedZone ? "Restricted Zone Alpha" : "Normal Waters")
                .setDistanceToPort(distanceToPort)
                .setRiskScore(riskScore)
                .setRiskLevel(riskLevel)
                .build();

        // 5. Send to enriched topic
        kafkaTemplate.send(ENRICHED_TOPIC, event.getMmsi(), enrichedEvent).whenComplete((result2, ex) -> {
            if (ex == null) {
                // Only acknowledge after the side-effect (produce) succeeds.
                // With MANUAL_IMMEDIATE ack mode this commits the offset,
                // guaranteeing at-least-once delivery.
                ack.acknowledge();
                System.out.println("Sent enriched event for MMSI: " + event.getMmsi() + " with risk level: " + enrichedEvent.getRiskLevel());
            } else {
                System.err.println("Failed to send enriched event for MMSI " + event.getMmsi() + ": " + ex.getMessage());
                // Don't ack — the record will be redelivered.
            }
        });
    }

    /**
     * Quarantine a vessel event with a reason header.
     * The event is published to the quarantine topic so it can be inspected later
     * without blocking the main pipeline.
     */
    private void quarantine(VesselEvent event, String reason) {
        ProducerRecord<String, VesselEvent> record = new ProducerRecord<>(QUARANTINE_TOPIC, event.getMmsi(), event);
        record.headers().add(REASON_HEADER, reason.getBytes(StandardCharsets.UTF_8));
        quarantineKafkaTemplate.send(record).whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Quarantined MMSI: " + event.getMmsi() + " reason: " + reason);
            } else {
                System.err.println("Failed to quarantine MMSI " + event.getMmsi() + ": " + ex.getMessage());
            }
        });
    }
}
