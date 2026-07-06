package com.maritime.api.service;

import com.maritime.api.dto.VesselDailySummary;
import com.maritime.api.repository.VesselHistoryRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Optional;

/**
 * Orchestrates the two views the API serves: the real-time hot-tier state (fetched from
 * the storage service over HTTP) and the Spark batch history (read from PostGIS via
 * {@link VesselHistoryRepository}).
 *
 * <p>Placing this between the controller and the data sources is the fix for audit C2:
 * the HTTP layer no longer speaks SQL or HTTP-to-storage directly, the query logic is
 * testable in isolation, and this is the correct layer on which to add a caching
 * boundary for the batch history.
 */
@Service
public class VesselIntelligenceService {

    private final RestTemplate            restTemplate;
    private final String                  storageServiceUrl;
    private final VesselHistoryRepository historyRepository;

    public VesselIntelligenceService(
            @Value("${maritime.storage.service.url:http://localhost:8083}") String storageServiceUrl,
            VesselHistoryRepository historyRepository) {
        this.restTemplate      = new RestTemplate();
        this.storageServiceUrl = storageServiceUrl;
        this.historyRepository = historyRepository;
    }

    /**
     * Latest real-time enriched event for a vessel, as canonical Avro-JSON from the
     * storage hot tier.
     *
     * <p>The storage service already returns canonical JSON, so it is forwarded
     * unchanged — the previous deserialize-then-re-serialize round-trip added two Avro
     * codec passes per call for a byte-identical result and is removed here (audit M1).
     *
     * @return the JSON payload, or empty if the vessel is unknown / has no hot-tier state.
     */
    public Optional<String> findRealTime(String mmsi) {
        try {
            String json = restTemplate.getForObject(
                    storageServiceUrl + "/api/v1/vessels/" + mmsi, String.class);
            return (json == null || json.isBlank()) ? Optional.empty() : Optional.of(json);
        } catch (HttpClientErrorException.NotFound e) {
            return Optional.empty();
        }
    }

    /**
     * Spark-computed daily history for a vessel (most recent first); empty when no batch
     * data exists yet.
     *
     * <p>This is the natural home for a caching boundary — the batch rollup only changes
     * when the Spark jobs run, so a {@code @Cacheable("vesselHistory")} keyed on
     * {@code mmsi} would fit here. It is deliberately left uncached for now so this
     * layering change does not also introduce a cache-eviction policy; enabling it is a
     * one-annotation follow-up once an eviction/TTL story is chosen.
     */
    public List<VesselDailySummary> findHistory(String mmsi) {
        return historyRepository.findHistory(mmsi);
    }
}
