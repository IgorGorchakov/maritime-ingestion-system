package com.maritime.enricher.service;

import com.maritime.common.dto.EnrichedHexCrossingEvent;
import com.maritime.common.dto.HexCrossingEvent;
import com.maritime.common.geo.GeoUtils;
import com.maritime.common.kafka.Topics;
import com.maritime.common.risk.RiskPolicy;
import com.maritime.enricher.geo.ZoneRepository;
import com.maritime.enricher.geo.ZoneRepository.ZoneView;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;

/**
 * Stateless enricher for raw H3 cell crossing events.
 *
 * <p>Consumes {@link HexCrossingEvent} from {@code maritime.hex.crossings},
 * looks up the zone containing the destination cell's centroid via PostGIS,
 * computes a risk score, and publishes an {@link EnrichedHexCrossingEvent} to
 * {@code maritime.hex.crossings.enriched}.
 *
 * <p>Reuses {@link ZoneRepository} and {@link PortDistanceProvider} — the same
 * infrastructure as {@link RiskScorerEnrichService}.
 */
@Slf4j
@Service
public class HexCrossingEnricherService {

    private static final List<String> ZONE_PRIORITY = List.of("EEZ", "PORT", "RESTRICTED");

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ZoneRepository                zoneRepository;
    private final PortDistanceProvider          portDistanceProvider;

    @Autowired
    public HexCrossingEnricherService(KafkaTemplate<String, Object> kafkaTemplate,
                                      ZoneRepository zoneRepository,
                                      PortDistanceProvider portDistanceProvider) {
        this.kafkaTemplate        = kafkaTemplate;
        this.zoneRepository       = zoneRepository;
        this.portDistanceProvider = portDistanceProvider;
    }

    @KafkaListener(topics = Topics.HEX_CROSSINGS, groupId = "hex-enricher-service")
    public void enrichCrossing(HexCrossingEvent event, Acknowledgment ack) {
        String mmsi = event.getMmsi();
        log.info("Enriching hex crossing mmsi={} from={} to={}", mmsi, event.getFromCell(), event.getToCell());

        double[] centroid = GeoUtils.h3CellCentroid(event.getToCell());
        double   lat      = centroid[0];
        double   lon      = centroid[1];

        List<ZoneView> zones      = zoneRepository.findZonesContaining(lat, lon);
        ZoneView       zone       = selectHighestPriorityZone(zones);
        boolean        restricted = zone != null && "RESTRICTED".equals(zone.getZone_type());
        String         zoneName   = zone != null ? zone.getName()      : null;
        String         zoneType   = zone != null ? zone.getZone_type() : null;
        double         distToPort = portDistanceProvider.distanceToNearestPortNm(lat, lon);

        double riskScore = 0.0;
        if (restricted)                    riskScore += RiskPolicy.RESTRICTED_ZONE_WEIGHT;
        else if ("PORT".equals(zoneType))  riskScore += RiskPolicy.PORT_ZONE_WEIGHT;
        else if ("EEZ".equals(zoneType))   riskScore += RiskPolicy.EEZ_ZONE_WEIGHT;
        if (distToPort < RiskPolicy.NEAR_PORT_THRESHOLD_NM) riskScore += RiskPolicy.NEAR_PORT_WEIGHT;
        String riskLevel = RiskPolicy.toRiskLevel(riskScore);

        EnrichedHexCrossingEvent enriched = EnrichedHexCrossingEvent.newBuilder()
                .setHexCrossingEvent(event)
                .setToCellCentroidLat(lat)
                .setToCellCentroidLon(lon)
                .setInRestrictedZone(restricted)
                .setZoneName(zoneName)
                .setZoneType(zoneType)
                .setDistanceToPort(distToPort)
                .setRiskScore(riskScore)
                .setRiskLevel(riskLevel)
                .build();

        kafkaTemplate.send(Topics.HEX_CROSSINGS_ENRICHED, mmsi, enriched)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        ack.acknowledge();
                        log.debug("Published enriched crossing mmsi={} riskLevel={}", mmsi, riskLevel);
                    } else {
                        log.error("Failed to publish enriched crossing mmsi={}", mmsi, ex);
                    }
                });
    }

    private ZoneView selectHighestPriorityZone(List<ZoneView> zones) {
        return zones.stream()
                .max(Comparator.comparingInt(z -> ZONE_PRIORITY.indexOf(z.getZone_type())))
                .orElse(null);
    }
}
