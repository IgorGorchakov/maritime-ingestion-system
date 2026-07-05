package com.maritime.enricher.service;

import com.maritime.common.dto.EnrichedVesselEvent;
import com.maritime.common.dto.VesselEvent;
import com.maritime.common.kafka.Topics;
import com.maritime.common.validation.ValidationResult;
import com.maritime.common.validation.VesselEventValidator;
import com.maritime.enricher.geo.ZoneRepository;
import com.maritime.enricher.geo.ZoneRepository.ZoneView;
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

/**
 * Validates, deduplicates, and enriches raw AIS events before publishing to
 * {@code maritime.enriched}. This service is stateless — it holds no per-vessel
 * history and uses no Kafka Streams API.
 *
 * <h3>ETL stages</h3>
 * <ol>
 *   <li><b>Extract</b> — event arrives from one of three raw source topics
 *       ({@code maritime.ais.raw.terrestrial}, {@code maritime.ais.raw.satellite},
 *       {@code maritime.ais.raw.vessel}) via a single {@code @KafkaListener} in the
 *       {@code enricher-service} consumer group. All three sources feed the same
 *       ETL pipeline and merge into {@code maritime.enriched}.</li>
 *   <li><b>Validate</b> — {@link VesselEventValidator} checks MMSI format,
 *       lat/lon bounds, null-island, timestamp freshness, and speed ceiling.
 *       Invalid events are routed to {@code maritime.ais.quarantine}.</li>
 *   <li><b>Dedup</b> — {@link DedupService} (Caffeine cache) drops replayed
 *       {@code (mmsi, timestamp)} pairs within the configured TTL window.
 *       Duplicates are quarantined for audit.</li>
 *   <li><b>Transform</b> — PostGIS {@link ZoneRepository} identifies which
 *       geofence zones contain the position; the highest-priority zone drives
 *       the risk score. {@link PortDistanceProvider} supplies the distance to
 *       the nearest port in nautical miles.</li>
 *   <li><b>Load</b> — the enriched event is published to
 *       {@code maritime.enriched}. The Kafka offset is committed only after
 *       the produce callback confirms durability (ack-after-side-effect).</li>
 * </ol>
 *
 * <h3>Detection flags</h3>
 * {@code loitering}, {@code darkVessel}, and {@code speedAnomaly} are always
 * initialised to {@code false} here. Behavioural detection runs in the separate
 * {@code maritime-detection} service ({@code MaritimeTopology}), which consumes
 * {@code maritime.enriched} under its own consumer group
 * ({@code maritime-detection-topology}) and publishes flagged events to the
 * dedicated {@code maritime.detections} topic. There is no feedback path back
 * into this service.
 */
@Slf4j
@Service
public class RiskScorerEnrichService {

    private static final String REASON_HEADER = "reason";

    // Zone type priority for risk scoring (higher index = higher risk weight).
    private static final List<String> ZONE_PRIORITY = List.of("EEZ", "PORT", "RESTRICTED");

    private final KafkaTemplate<String, Object>    kafkaTemplate;
    private final KafkaTemplate<String, VesselEvent> quarantineKafkaTemplate;
    private final VesselEventValidator             validator;
    private final DedupService                     dedupService;
    private final ZoneRepository                   zoneRepository;
    private final PortDistanceProvider             portDistanceProvider;
    private final MeterRegistry                    meterRegistry;
    private final Counter                          eventsEnriched;
    private final Timer                            processingLatency;

    @Autowired
    public RiskScorerEnrichService(KafkaTemplate<String, Object> kafkaTemplate,
                                   KafkaTemplate<String, VesselEvent> quarantineKafkaTemplate,
                                   VesselEventValidator validator,
                                   DedupService dedupService,
                                   ZoneRepository zoneRepository,
                                   PortDistanceProvider portDistanceProvider,
                                   MeterRegistry meterRegistry) {
        this.kafkaTemplate           = kafkaTemplate;
        this.quarantineKafkaTemplate = quarantineKafkaTemplate;
        this.validator               = validator;
        this.dedupService            = dedupService;
        this.zoneRepository          = zoneRepository;
        this.portDistanceProvider    = portDistanceProvider;
        this.meterRegistry           = meterRegistry;
        this.eventsEnriched = Counter.builder("events.enriched")
                .description("Events successfully enriched and published to maritime.enriched")
                .register(meterRegistry);
        this.processingLatency = Timer.builder("event.processing.latency")
                .description("End-to-end time to validate, score, and publish one event")
                .register(meterRegistry);
    }

    @KafkaListener(topics = {
            Topics.AIS_RAW_TERRESTRIAL,
            Topics.AIS_RAW_SATELLITE,
            Topics.AIS_RAW_VESSEL
    }, groupId = "enricher-service")
    public void consumeAndScore(VesselEvent event, Acknowledgment ack) {
        Timer.Sample timer = Timer.start(meterRegistry);
        log.info("Received event for MMSI: {}", event.getMmsi());

        // ── Validate ──────────────────────────────────────────────────────────
        ValidationResult result = validator.validate(event);
        if (!result.isValid()) {
            quarantine(event, result.getReason(), ack);
            timer.stop(processingLatency);
            return;
        }

        // ── Dedup ─────────────────────────────────────────────────────────────
        String mmsi      = event.getMmsi();
        long   timestamp = event.getTimestamp().toEpochMilli();
        if (dedupService.isDuplicate(mmsi, timestamp)) {
            quarantine(event, "duplicate", ack);
            timer.stop(processingLatency);
            return;
        }

        // ── Enrich: PostGIS zone lookup + risk scoring ─────────────────────
        double lat = event.getLatitude();
        double lon = event.getLongitude();

        // ST_Contains may return multiple overlapping zones.
        List<ZoneView> zones = zoneRepository.findZonesContaining(lat, lon);

        // Select the highest-priority zone for the risk label; fall back to null.
        ZoneView primaryZone = zones.stream()
                .max(Comparator.comparingInt(z -> ZONE_PRIORITY.indexOf(z.getZone_type())))
                .orElse(null);

        boolean inRestrictedZone = zones.stream()
                .anyMatch(z -> "RESTRICTED".equals(z.getZone_type()));
        String zoneName = primaryZone != null ? primaryZone.getName()      : null;
        String zoneType = primaryZone != null ? primaryZone.getZone_type() : null;

        // PortDistanceProvider is injected via PipelineConfig; currently backed by
        // RandomPortDistanceProvider (placeholder). Phase 7 swaps in
        // PostGisPortDistanceProvider — no change required here.
        double distanceToPort = portDistanceProvider.distanceToNearestPortNm(lat, lon);

        double riskScore = 0.0;
        if (inRestrictedZone)             riskScore += 50;
        else if ("PORT".equals(zoneType)) riskScore += 20;
        else if ("EEZ".equals(zoneType))  riskScore += 10;
        if (distanceToPort < 10)          riskScore += 20;
        if (event.getSpeed() > 25)        riskScore += 10;

        String riskLevel = riskScore >= 50 ? "HIGH" : riskScore >= 20 ? "MEDIUM" : "LOW";
        meterRegistry.counter("risk.level", "level", riskLevel).increment();

        // ── Publish to maritime.enriched ──────────────────────────────────────
        // Detection flags are always false at this stage. The maritime-detection
        // service (MaritimeTopology, group maritime-detection-topology) consumes
        // maritime.enriched, runs stateful detection, and publishes flagged events
        // to maritime.detections. These two topics are independent — no loop.
        EnrichedVesselEvent enriched = EnrichedVesselEvent.newBuilder()
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

        kafkaTemplate.send(Topics.ENRICHED, mmsi, enriched).whenComplete((res, ex) -> {
            if (ex == null) {
                ack.acknowledge();
                eventsEnriched.increment();
                log.info("Enriched MMSI {} → zone={} risk={}", mmsi, zoneName, riskLevel);
            } else {
                // No ack — DefaultErrorHandler retries; exhausted retries → DLT.
                log.error("Failed to publish enriched event for MMSI {}: {}", mmsi, ex.getMessage(), ex);
            }
            timer.stop(processingLatency);
        });
    }

    /**
     * Route a bad or duplicate event to the quarantine topic with a {@code reason} header.
     * Offset is committed only after the quarantine produce confirms — same
     * ack-after-side-effect pattern as the enriched path.
     */
    private void quarantine(VesselEvent event, String reason, Acknowledgment ack) {
        meterRegistry.counter("events.quarantined", "reason", reason).increment();
        ProducerRecord<String, VesselEvent> record =
                new ProducerRecord<>(Topics.QUARANTINE, event.getMmsi(), event);
        record.headers().add(REASON_HEADER, reason.getBytes(StandardCharsets.UTF_8));
        quarantineKafkaTemplate.send(record).whenComplete((res, ex) -> {
            if (ex == null) {
                ack.acknowledge();
                log.info("Quarantined MMSI {} reason={}", event.getMmsi(), reason);
            } else {
                // No ack — DefaultErrorHandler retries; exhausted retries → DLT.
                log.error("Failed to quarantine MMSI {}: {}", event.getMmsi(), ex.getMessage(), ex);
            }
        });
    }
}
