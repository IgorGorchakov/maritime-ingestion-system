package com.maritime.enricher.service;

import lombok.extern.slf4j.Slf4j;

import java.util.Random;

/**
 * Placeholder {@link PortDistanceProvider} that returns a uniform random value
 * in [0, 100) NM.
 *
 * <h3>Why this exists</h3>
 * A real distance-to-port lookup requires a port gazetteer — either a PostGIS
 * table populated from a GeoJSON dataset or an in-memory spatial index. That
 * data pipeline is deferred to Phase 7. This class holds the placeholder
 * logic that was previously inlined in {@code RiskScorerEnrichService}, making
 * the temporary nature explicit and the real implementation easy to swap in.
 *
 * <h3>Replacing this</h3>
 * Phase 7 will introduce {@code PostGisPortDistanceProvider}, which runs:
 * <pre>{@code
 * SELECT ST_Distance(
 *     ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
 *     geom::geography
 * ) / 1852.0  -- metres → nautical miles
 * FROM ports
 * ORDER BY geom <-> ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)
 * LIMIT 1
 * }</pre>
 * To activate it, replace the {@link com.maritime.enricher.config.PipelineConfig#portDistanceProvider()}
 * bean body — no other file needs to change.
 *
 * <h3>Thread safety</h3>
 * A new {@link Random} instance is created per call, which is safe but slightly
 * wasteful. Acceptable for a placeholder; the real implementation will use a
 * thread-safe PostGIS query via Spring's {@code JdbcTemplate}.
 */
@Slf4j
public class RandomPortDistanceProvider implements PortDistanceProvider {

    @Override
    public double distanceToNearestPortNm(double latitudeDeg, double longitudeDeg) {
        double distanceNm = new Random().nextDouble() * 100;
        log.debug("RandomPortDistanceProvider: placeholder distance {} NM for ({}, {}) "
                + "— replace with PostGisPortDistanceProvider in Phase 7",
                String.format("%.1f", distanceNm), latitudeDeg, longitudeDeg);
        return distanceNm;
    }
}
