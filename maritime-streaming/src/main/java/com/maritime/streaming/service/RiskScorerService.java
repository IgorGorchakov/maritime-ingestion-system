package com.maritime.streaming.service;

import com.maritime.common.dto.EnrichedVesselEvent;
import com.maritime.common.dto.VesselEvent;
import com.maritime.common.geo.GeoUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Random;

@Service
public class RiskScorerService {

    private static final String ENRICHED_TOPIC = "maritime.enriched";
    private final KafkaTemplate<String, EnrichedVesselEvent> kafkaTemplate;
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
    public RiskScorerService(KafkaTemplate<String, EnrichedVesselEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "maritime.ais.raw", groupId = "streaming-service")
    public void consumeAndScore(VesselEvent event) {
        System.out.println("Received event for MMSI: " + event.getMmsi());

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
        kafkaTemplate.send(ENRICHED_TOPIC, event.getMmsi(), enrichedEvent);
        System.out.println("Sent enriched event for MMSI: " + event.getMmsi() + " with risk level: " + riskLevel);
    }
}