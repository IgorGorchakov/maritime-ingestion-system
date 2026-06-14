package com.maritime.storage.controller;

import com.maritime.common.dto.EnrichedVesselEvent;
import com.maritime.common.dto.VesselEvent;
import com.maritime.common.kafka.Topics;
import com.maritime.common.serde.AvroJson;
import com.maritime.storage.service.ColdTierWriter;
import com.maritime.storage.service.VesselStateStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.web.bind.annotation.*;

/**
 * Consumes enriched vessel events from Kafka and persists them to:
 * <ul>
 *   <li><b>Cold tier (Parquet on the local filesystem):</b> one file per event,
 *       partitioned by {@code date=/mmsi=} for Spark partition pruning (Phase 7).</li>
 *   <li><b>Hot tier (Postgres):</b> latest state per MMSI for real-time REST queries.</li>
 * </ul>
 *
 * <p>The controller depends only on the {@link ColdTierWriter} and
 * {@link VesselStateStore} storage ports — it never references the concrete backing
 * store, so swapping filesystem/Postgres for S3/DynamoDB (or anything else) needs no
 * change here.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1")
public class VesselController {

    private final ColdTierWriter coldTier;
    private final VesselStateStore stateStore;

    public VesselController(ColdTierWriter coldTier, VesselStateStore stateStore) {
        this.coldTier   = coldTier;
        this.stateStore = stateStore;
    }

    /**
     * Consumes base enriched events (zone + risk, no detection flags).
     * This is the primary cold + hot tier write path for every vessel event.
     */
    @KafkaListener(topics = Topics.ENRICHED, groupId = "storage-service")
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
     * We upsert into the same Postgres hot tier so a GET /vessels/{mmsi}
     * immediately reflects the latest detection state.
     */
    @KafkaListener(topics = Topics.DETECTIONS, groupId = "storage-service")
    public void consumeDetectionEvent(EnrichedVesselEvent event, Acknowledgment ack) {
        persist(event, ack);
    }

    /**
     * Shared persistence logic for both listeners.
     * Writes to the cold tier (Parquet) and hot tier (Postgres), then acks.
     * Any exception propagates to DefaultErrorHandler (retry → DLT).
     */
    private void persist(EnrichedVesselEvent event, Acknowledgment ack) {
        VesselEvent vessel = event.getVesselEvent();

        coldTier.write(event);     // cold tier: Parquet, partitioned by date=/mmsi=
        stateStore.upsert(event);  // hot tier: latest state per MMSI

        // Offset committed only after both writes succeed.
        ack.acknowledge();
        log.info("Persisted MMSI={} riskLevel={} loitering={} dark={} speedAnomaly={}",
                vessel.getMmsi(), event.getRiskLevel(),
                event.getLoitering(), event.getDarkVessel(), event.getSpeedAnomaly());
    }

    @GetMapping(value = "/vessels/{mmsi}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> getVesselRisk(@PathVariable String mmsi) {
        return stateStore.findByMmsi(mmsi)
                .map(AvroJson::toJson)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
