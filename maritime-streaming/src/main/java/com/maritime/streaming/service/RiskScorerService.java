package com.maritime.streaming.service;

import com.maritime.common.dto.EnrichedVesselEvent;
import com.maritime.common.dto.VesselEvent;
import com.maritime.common.validation.ValidationResult;
import com.maritime.common.validation.VesselEventValidator;
import com.maritime.streaming.geo.ZoneRepository;
import com.maritime.streaming.geo.ZoneRepository.ZoneView;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

/**
 * Validates, deduplicates, and enriches raw AIS events before publishing to
 * {@code maritime.enriched}.
 *
 * <h3>Phase 6 changes</h3>
 * <ul>
 *   <li>Replaced the hardcoded rectangle with a PostGIS {@link ZoneRepository}
 *       lookup ({@code ST_Contains}). The "most significant" zone is selected by
 *       priority: RESTRICTED > PORT > EEZ.</li>
 *   <li>The new {@code zoneType} field on {@link EnrichedVesselEvent} is populated.</li>
 *   <li>Detection flags (loitering/darkVessel/speedAnomaly) default to {@code false}
 *       here — {@link com.maritime.streaming.streams.MaritimeTopology} sets them
 *       on the downstream re-enrichment pass.</li>
 * </ul>
 */
@Slf4j
@Service
public class RiskScorerService {

    private static final String ENRICHED_TOPIC   = "maritime.enriched";
    private static final String QUARANTINE_TOPIC = "maritime.ais.quarantine";
    private static final String REASON_HEADER    = "reason";

    // Zone type priority for risk scoring (higher index = higher risk weight).
    private static final List<String> ZONE_PRIORITY = List.of("EEZ", "PORT", "RESTRICTED");

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTemplate<String, VesselEvent> quarantineKafkaTemplate;
    private final VesselEventValidator validator;
    private final DedupService dedupService;
    private final ZoneRepository zoneRepository;
    private final MeterRegistry meterRegistry;
    private final Counter eventsEnriched;
    private final Timer processingLatency;
    private final Random random = new Random();

    @Autowired
    public RiskScorerService(KafkaTemplate<String, Object> kafkaTemplate,
                             KafkaTemplate<String, VesselEvent> quarantineKafkaTemplate,
                             VesselEventValidator validator,
                             DedupService dedupService,
                             ZoneRepository zoneRepository,
                             MeterRegistry meterRegistry) {
        this.kafkaTemplate           = kafkaTemplate;
        this.quarantineKafkaTemplate = quarantineKafkaTemplate;
        this.validator               = validator;
        this.dedupService            = dedupService;
        this.zoneRepository          = zoneRepository;
        this.meterRegistry           = meterRegistry;
        this.eventsEnriched = Counter.builder("events.enriched")
                .description("Events successfully enriched and published to the enriched topic")
                .register(meterRegistry);
        this.processingLatency = Timer.builder("event.processing.latency")
                .description("End-to-end time to validate, score, and publish one event")
                .register(meterRegistry);
    }

    @KafkaListener(topics = "maritime.ais.raw", groupId = "streaming-service")
    public void consumeAndScore(VesselEvent event, Acknowledgment ack) {
        Timer.Sample sample = Timer.start(meterRegistry);
        log.info("Received event for MMSI: {}", event.getMmsi());

        // ── Validate (E → V) ─────────────────────────────────────────────────
        ValidationResult result = validator.validate(event);
        if (!result.isValid()) {
            quarantine(event, result.getReason(), ack);
            sample.stop(processingLatency);
            return;
        }

        // ── Dedup (V → T guard) ──────────────────────────────────────────────
        String mmsi      = event.getMmsi();
        long   timestamp = event.getTimestamp().toEpochMilli();
        if (dedupService.isDuplicate(mmsi, timestamp)) {
            quarantine(event, "duplicate", ack);
            sample.stop(processingLatency);
            return;
        }

        // ── Transform (T): PostGIS zone lookup + risk scoring ─────────────────
        double lat = event.getLatitude();
        double lon = event.getLongitude();

        // ST_Contains query — may return multiple overlapping zones.
        List<ZoneView> zones = zoneRepository.findZonesContaining(lat, lon);

        // Select the highest-priority zone for labelling; fall back to null.
        ZoneView primaryZone = zones.stream()
                .max(Comparator.comparingInt(z -> ZONE_PRIORITY.indexOf(z.getZone_type())))
                .orElse(null);

        boolean inRestrictedZone = zones.stream()
                .anyMatch(z -> "RESTRICTED".equals(z.getZone_type()));
        String zoneName = primaryZone != null ? primaryZone.getName()       : null;
        String zoneType = primaryZone != null ? primaryZone.getZone_type()  : null;

        // Distance to port: random placeholder — Phase 7 replaces with real lookup.
        double distanceToPort = random.nextDouble() * 100;

        // Risk scoring: zone type drives the base score.
        double riskScore = 0.0;
        if (inRestrictedZone)          riskScore += 50;
        else if ("PORT".equals(zoneType)) riskScore += 20;
        else if ("EEZ".equals(zoneType))  riskScore += 10;

        if (distanceToPort < 10) riskScore += 20;
        if (event.getSpeed() > 25) riskScore += 10;

        String riskLevel = riskScore >= 50 ? "HIGH" : riskScore >= 20 ? "MEDIUM" : "LOW";
        meterRegistry.counter("risk.level", "level", riskLevel).increment();

        // ── Load (L): publish enriched event ─────────────────────────────────
        // Detection flags (loitering/darkVessel/speedAnomaly) start as false here.
        // MaritimeTopology consumes from maritime.enriched and sets them on a
        // second pass, re-publishing the flagged event to the same topic.
        EnrichedVesselEvent enrichedEvent = EnrichedVesselEvent.newBuilder()
                .setVesselEvent(event)
                .setInRestrictedZone(inRestrictedZone)
                .setZoneName(zoneName)
                .setZoneType(zoneType)
                .setDistanceToPort(distanceToPort)
                .setRiskScore(riskScore)
                .setRiskLevel(riskLevel)
                .setLoitering(false)
                .setDarkVessel(false)
                .setSpeedAnomaly(false)
                .build();

        kafkaTemplate.send(ENRICHED_TOPIC, mmsi, enrichedEvent).whenComplete((res, ex) -> {
            if (ex == null) {
                ack.acknowledge();
                eventsEnriched.increment();
                log.info("Enriched MMSI {} → zone={} risk={}", mmsi, zoneName, riskLevel);
            } else {
                log.error("Failed to send enriched event for MMSI {}: {}", mmsi, ex.getMessage(), ex);
                // No ack — record will be redelivered by DefaultErrorHandler.
            }
            sample.stop(processingLatency);
        });
    }

    /**
     * Route a bad/duplicate event to the quarantine topic.
     * Ack deferred into the produce callback — offset is only committed after
     * the quarantine record is durably handed off to Kafka.
     */
    private void quarantine(VesselEvent event, String reason, Acknowledgment ack) {
        meterRegistry.counter("events.quarantined", "reason", reason).increment();
        ProducerRecord<String, VesselEvent> record =
                new ProducerRecord<>(QUARANTINE_TOPIC, event.getMmsi(), event);
        record.headers().add(REASON_HEADER, reason.getBytes(StandardCharsets.UTF_8));
        quarantineKafkaTemplate.send(record).whenComplete((res, ex) -> {
            if (ex == null) {
                ack.acknowledge();
                log.info("Quarantined MMSI {} reason: {}", event.getMmsi(), reason);
            } else {
                // No ack — DefaultErrorHandler retries; exhausted retries → DLT.
                log.error("Failed to quarantine MMSI {}: {}", event.getMmsi(), ex.getMessage(), ex);
            }
        });
    }
}
