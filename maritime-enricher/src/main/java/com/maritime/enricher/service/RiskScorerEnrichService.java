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
 * {@code maritime.enriched}.
 *
 * <h3>ETL stages</h3>
 * <ol>
 *   <li><b>Extract</b> — event arrives from {@code maritime.ais.raw} via
 *       {@code @KafkaListener}.</li>
 *   <li><b>Validate</b> — {@link VesselEventValidator} checks MMSI format,
 *       lat/lon bounds, null-island, timestamp freshness, and speed ceiling.
 *       Invalid events are routed to {@code maritime.ais.quarantine}.</li>
 *   <li><b>Dedup</b> — {@link DedupService} (Caffeine cache) drops replayed
 *       {@code (mmsi, timestamp)} pairs within the configured TTL window.
 *       Duplicates are also quarantined so they can be audited.</li>
 *   <li><b>Transform</b> — PostGIS {@link ZoneRepository} identifies which
 *       geofence zones contain the position; the highest-priority zone drives
 *       the risk score. {@link PortDistanceProvider} supplies the distance to
 *       the nearest port in nautical miles.</li>
 *   <li><b>Load</b> — the enriched event is published to
 *       {@code maritime.enriched}. The Kafka offset is only committed (ack)
 *       after the produce callback confirms durability.</li>
 * </ol>
 *
 * <h3>Detection flags</h3>
 * {@code loitering}, {@code darkVessel}, and {@code speedAnomaly} are
 * initialised to {@code false} here. They are set by
 * {@link com.maritime.enricher.streams.MaritimeTopology}, which consumes
 * from {@code maritime.enriched} with its own consumer group and re-publishes
 * flagged events to the <em>separate</em> {@code maritime.detections} topic.
 * The two topics are independent — there is no feedback loop back into this
 * service.
 *
 * <h3>Phase 6 changes</h3>
 * <ul>
 *   <li>Replaced the hardcoded rectangle with a PostGIS {@link ZoneRepository}
 *       lookup ({@code ST_Contains}). The "most significant" zone is selected
 *       by priority: RESTRICTED &gt; PORT &gt; EEZ.</li>
 *   <li>The new {@code zoneType} field on {@link EnrichedVesselEvent} is
 *       populated.</li>
 *   <li>Distance-to-port extracted behind {@link PortDistanceProvider} so the
 *       placeholder can be swapped for a real PostGIS lookup in Phase 7 without
 *       touching this class.</li>
 * </ul>
 */
@Slf4j
@Service
public class RiskScorerEnrichService {

    private static final String REASON_HEADER = "reason";

    // Zone type priority for risk scoring (higher index = higher risk weight).
    private static final List<String> ZONE_PRIORITY = List.of("EEZ", "PORT", "RESTRICTED");

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTemplate<String, VesselEvent> quarantineKafkaTemplate;
    private final VesselEventValidator validator;
    private final DedupService dedupService;
    private final ZoneRepository zoneRepository;
    private final PortDistanceProvider portDistanceProvider;
    private final MeterRegistry meterRegistry;
    private final Counter eventsEnriched;
    private final Timer processingLatency;

    @Autowired
    public RiskScorerEnrichService(KafkaTemplate<String, Object> kafkaTemplate,
                                   KafkaTemplate<String, VesselEvent> quarantineKafkaTemplate,
                                   VesselEventValidator validator,
                                   DedupService dedupService,
                                   ZoneRepository zoneRepository,
                                   PortDistanceProvider portDistanceProvider,
                                   MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.quarantineKafkaTemplate = quarantineKafkaTemplate;
        this.validator = validator;
        this.dedupService = dedupService;
        this.zoneRepository = zoneRepository;
        this.portDistanceProvider = portDistanceProvider;
        this.meterRegistry = meterRegistry;
        this.eventsEnriched = Counter.builder("events.enriched")
                .description("Events successfully enriched and published to the enriched topic")
                .register(meterRegistry);
        this.processingLatency = Timer.builder("event.processing.latency")
                .description("End-to-end time to validate, score, and publish one event")
                .register(meterRegistry);
    }

    @KafkaListener(topics = Topics.AIS_RAW, groupId = "streaming-service")
    public void consumeAndScore(VesselEvent event, Acknowledgment ack) {
        Timer.Sample timer = Timer.start(meterRegistry);
        log.info("Received event for MMSI: {}", event.getMmsi());

        // ── Validate (E → V) ─────────────────────────────────────────────────
        ValidationResult result = validator.validate(event);
        if (!result.isValid()) {
            quarantine(event, result.getReason(), ack);
            timer.stop(processingLatency);
            return;
        }

        // ── Dedup (V → T guard) ──────────────────────────────────────────────
        String mmsi = event.getMmsi();
        long timestamp = event.getTimestamp().toEpochMilli();
        if (dedupService.isDuplicate(mmsi, timestamp)) {
            quarantine(event, "duplicate", ack);
            timer.stop(processingLatency);
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
        String zoneName = primaryZone != null ? primaryZone.getName() : null;
        String zoneType = primaryZone != null ? primaryZone.getZone_type() : null;

        // Delegate distance-to-port to the injected strategy. The current
        // implementation is RandomPortDistanceProvider (placeholder). Phase 7
        // replaces it with PostGisPortDistanceProvider via PipelineConfig —
        // no changes required here.
        double distanceToPort = portDistanceProvider.distanceToNearestPortNm(lat, lon);

        // Risk scoring: zone type drives the base score.
        double riskScore = 0.0;
        if (inRestrictedZone) riskScore += 50;
        else if ("PORT".equals(zoneType)) riskScore += 20;
        else if ("EEZ".equals(zoneType)) riskScore += 10;

        if (distanceToPort < 10) riskScore += 20;
        if (event.getSpeed() > 25) riskScore += 10;

        String riskLevel = riskScore >= 50 ? "HIGH" : riskScore >= 20 ? "MEDIUM" : "LOW";
        meterRegistry.counter("risk.level", "level", riskLevel).increment();

        // ── Load (L): publish enriched event ─────────────────────────────────
        // Detection flags default to false. MaritimeTopology consumes from
        // maritime.enriched under its own consumer group ("maritime-detection-topology")
        // and publishes any flagged events to the SEPARATE maritime.detections topic.
        // That topic is consumed by the storage service independently — there is no
        // path by which a detection re-enters this service or maritime.enriched.
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

        kafkaTemplate.send(Topics.ENRICHED, mmsi, enrichedEvent).whenComplete((res, ex) -> {
            if (ex == null) {
                ack.acknowledge();
                eventsEnriched.increment();
                log.info("Enriched MMSI {} → zone={} risk={}", mmsi, zoneName, riskLevel);
            } else {
                log.error("Failed to send enriched event for MMSI {}: {}", mmsi, ex.getMessage(), ex);
                // No ack — record will be redelivered by DefaultErrorHandler.
            }
            timer.stop(processingLatency);
        });
    }

    /**
     * Route a bad/duplicate event to the quarantine topic.
     * The Kafka offset is only committed after the produce callback confirms the
     * quarantine record is durably handed off — same ack-after-side-effect pattern
     * as the enriched path.
     */
    private void quarantine(VesselEvent event, String reason, Acknowledgment ack) {
        meterRegistry.counter("events.quarantined", "reason", reason).increment();
        ProducerRecord<String, VesselEvent> record =
                new ProducerRecord<>(Topics.QUARANTINE, event.getMmsi(), event);
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
