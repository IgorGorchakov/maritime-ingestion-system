package com.maritime.storage.service;

import com.maritime.common.dto.EnrichedVesselEvent;
import com.maritime.common.dto.VesselEvent;
import com.maritime.common.kafka.Topics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

/**
 * Consumes enriched vessel events from Kafka and persists them to both storage tiers:
 * <ul>
 *   <li><b>Cold tier</b> ({@link ColdTierWriter}) — one JSON file per event, partitioned
 *       {@code date=/mmsi=} so Spark can prune partitions without a metastore.</li>
 *   <li><b>Hot tier</b> ({@link VesselStateHotStore}) — latest state per MMSI for the
 *       real-time query API.</li>
 * </ul>
 *
 * <p>Depends only on the two storage ports, never the concrete backing store, so the
 * filesystem/Postgres implementations can be swapped with no change here. The REST query
 * side lives in {@link com.maritime.storage.controller.VesselQueryController}; the two
 * were split (audit H2) because the consumer concern (ack mode, retry, DLT routing) and
 * the query concern (caching, rate limiting) evolve independently.
 */
@Slf4j
@Service
public class VesselEventConsumer {

    private final ColdTierWriter coldTier;
    private final VesselStateHotStore hotTier;

    public VesselEventConsumer(ColdTierWriter coldTier, VesselStateHotStore hotTier) {
        this.coldTier = coldTier;
        this.hotTier  = hotTier;
    }

    /**
     * Consumes base enriched events (zone + risk, no detection flags) — the primary
     * cold + hot tier write path for every vessel event.
     */
    @KafkaListener(topics = Topics.ENRICHED, groupId = "storage-service")
    public void consumeEnrichedEvent(EnrichedVesselEvent event, Acknowledgment ack) {
        persist(event, ack);
    }

    /**
     * Consumes detection events from the topology's dedicated output topic.
     *
     * <p>{@code maritime.detections} receives only events where at least one flag fired
     * (loitering / darkVessel / speedAnomaly). Subscribing to a separate topic instead of
     * {@code maritime.enriched} means the topology's output can never feed back into its
     * own input — a processing loop is structurally impossible. Upserting into the same
     * hot tier means a {@code GET /vessels/{mmsi}} immediately reflects the latest
     * detection state.
     */
    @KafkaListener(topics = Topics.DETECTIONS, groupId = "storage-service")
    public void consumeDetectionEvent(EnrichedVesselEvent event, Acknowledgment ack) {
        persist(event, ack);
    }

    /**
     * Shared persistence for both listeners: writes the cold tier (JSON) and hot tier
     * (Postgres), then acks. Any exception propagates to {@code DefaultErrorHandler}
     * (retry → DLT); the offset is committed only after both writes succeed.
     */
    private void persist(EnrichedVesselEvent event, Acknowledgment ack) {
        VesselEvent vessel = event.getVesselEvent();

        coldTier.write(event);   // cold tier: JSON, partitioned by date=/mmsi=
        hotTier.upsert(event);   // hot tier: latest state per MMSI

        // Offset committed only after both writes succeed.
        ack.acknowledge();
        log.info("Persisted MMSI={} riskLevel={} loitering={} dark={} speedAnomaly={}",
                vessel.getMmsi(),
                event.getRiskLevel(),
                event.getLoitering(),
                event.getDarkVessel(),
                event.getSpeedAnomaly());
    }
}
