package com.maritime.api.controller;

import com.maritime.common.dto.EnrichedVesselEvent;
import com.maritime.common.serde.AvroJson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;

/**
 * Public API service for vessel intelligence.
 *
 * <h3>Endpoints</h3>
 * <ul>
 *   <li>{@code GET /api/v1/intelligence/{mmsi}} — latest real-time enriched event
 *       (from the Postgres hot tier via the storage service).</li>
 *   <li>{@code GET /api/v1/intelligence/{mmsi}/history} — Spark batch rollup:
 *       daily aggregates, risk percentiles, and detection flag counts computed
 *       over the Parquet cold tier (Phase 7, Lambda batch layer).</li>
 * </ul>
 *
 * <h3>Lambda architecture serving layer</h3>
 * The API service is the <em>serving layer</em> of the Lambda architecture:
 * <ul>
 *   <li><b>Speed layer</b> (MaritimeTopology) → Postgres hot tier → {@code /{mmsi}}</li>
 *   <li><b>Batch layer</b> (Spark jobs) → PostGIS → {@code /{mmsi}/history}</li>
 * </ul>
 * Both views use the same MMSI key; the client can merge them to get the full
 * historical + real-time picture.
 */
@RestController
@RequestMapping("/api/v1/intelligence")
public class ApiController {

    private final RestTemplate restTemplate;
    private final String storageServiceUrl;
    private final JdbcTemplate jdbc;

    @Autowired
    public ApiController(
            @Value("${maritime.storage.service.url:http://localhost:8083}") String storageServiceUrl,
            JdbcTemplate jdbc) {
        this.restTemplate    = new RestTemplate();
        this.storageServiceUrl = storageServiceUrl;
        this.jdbc            = jdbc;
    }

    // ── Real-time endpoint (unchanged) ────────────────────────────────────────

    @GetMapping(value = "/{mmsi}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> getVesselIntelligence(@PathVariable String mmsi) {
        String json = restTemplate.getForObject(
                storageServiceUrl + "/api/v1/vessels/" + mmsi, String.class);
        if (json == null || json.isBlank()) {
            return ResponseEntity.notFound().build();
        }
        EnrichedVesselEvent vesselData = AvroJson.fromJson(json, EnrichedVesselEvent.class);
        return ResponseEntity.ok(AvroJson.toJson(vesselData));
    }

    // ── Batch history endpoint (Phase 7) ─────────────────────────────────────

    /**
     * Returns the Spark-computed daily history for a vessel.
     *
     * <p>Merges three PostGIS tables written by the Spark batch jobs:
     * <ul>
     *   <li>{@code vessel_daily_stats} — event counts, speed, detection flags</li>
     *   <li>{@code vessel_risk_percentiles} — p50/p95 risk scores</li>
     * </ul>
     * Results are ordered by date descending (most recent first) and capped at 90 days.
     *
     * <p>Returns 404 if no batch data exists yet (Spark jobs haven't run).
     * The real-time {@code /{mmsi}} endpoint is always available regardless.
     *
     * <p>Response is a JSON array of daily summary objects:
     * <pre>
     * [
     *   {
     *     "date": "2024-01-15",
     *     "eventCount": 1440,
     *     "avgSpeedKn": 8.3,
     *     "maxSpeedKn": 18.1,
     *     "avgRiskScore": 32.5,
     *     "restrictedCount": 12,
     *     "loiteringCount": 45,
     *     "darkVesselCount": 0,
     *     "speedAnomalyCount": 3,
     *     "p50Risk": 28.0,
     *     "p95Risk": 75.0
     *   },
     *   ...
     * ]
     * </pre>
     */
    @GetMapping(value = "/{mmsi}/history", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<Map<String, Object>>> getVesselHistory(@PathVariable String mmsi) {
        // LEFT JOIN vessel_risk_percentiles so rows with no percentile data
        // (e.g. only DailyVesselAggregatesJob ran, not RiskRollupJob) still appear.
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT
                    s.date::text                        AS date,
                    s.event_count                       AS "eventCount",
                    ROUND(s.avg_speed_kn::numeric, 2)   AS "avgSpeedKn",
                    ROUND(s.max_speed_kn::numeric, 2)   AS "maxSpeedKn",
                    ROUND(s.avg_risk_score::numeric, 2) AS "avgRiskScore",
                    s.restricted_count                  AS "restrictedCount",
                    s.loitering_count                   AS "loiteringCount",
                    s.dark_vessel_count                 AS "darkVesselCount",
                    s.speed_anomaly_count               AS "speedAnomalyCount",
                    ROUND(p.p50_risk::numeric, 2)       AS "p50Risk",
                    ROUND(p.p95_risk::numeric, 2)       AS "p95Risk"
                FROM vessel_daily_stats s
                LEFT JOIN vessel_risk_percentiles p
                    ON s.mmsi = p.mmsi AND s.date = p.date
                WHERE s.mmsi = ?
                ORDER BY s.date DESC
                LIMIT 90
                """, mmsi);

        if (rows.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(rows);
    }
}
