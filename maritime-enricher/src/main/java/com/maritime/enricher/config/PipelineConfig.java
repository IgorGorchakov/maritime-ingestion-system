package com.maritime.enricher.config;

import com.maritime.common.validation.VesselEventValidator;
import com.maritime.enricher.service.DedupService;
import com.maritime.enricher.service.PortDistanceProvider;
import com.maritime.enricher.service.RandomPortDistanceProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Wires the pipeline collaborators — validator, dedup, and port-distance
 * provider — as Spring beans.
 *
 * <p>All three are plain classes by design (no Spring annotations on the
 * implementation types), so they are registered here via {@code @Bean} rather
 * than component-scanned. This keeps the implementations framework-free and
 * directly unit-testable.
 *
 * <h3>Swapping the port-distance provider</h3>
 * The current {@link RandomPortDistanceProvider} is a placeholder.
 * Phase 7 introduces {@code PostGisPortDistanceProvider}. To activate it,
 * replace the {@link #portDistanceProvider()} bean body — no other file needs
 * to change because {@link com.maritime.enricher.service.RiskScorerService}
 * depends on the {@link PortDistanceProvider} interface, not the implementation.
 */
@Configuration
public class PipelineConfig {

    /**
     * Dedup window: a (mmsi, timestamp) key is treated as a duplicate if seen
     * again within this TTL. Defaults to 1 hour; override with
     * {@code dedup.ttl-minutes} in application properties.
     */
    @Value("${dedup.ttl-minutes:60}")
    private long dedupTtlMinutes;

    @Bean
    public VesselEventValidator vesselEventValidator() {
        return new VesselEventValidator();
    }

    @Bean
    public DedupService dedupService() {
        return new DedupService(Duration.ofMinutes(dedupTtlMinutes));
    }

    /**
     * Port-distance strategy used by {@link com.maritime.enricher.service.RiskScorerService}.
     *
     * <p>Currently wired to {@link RandomPortDistanceProvider} (placeholder).
     * Phase 7 replaces this with {@code new PostGisPortDistanceProvider(jdbcTemplate)}
     * once the port gazetteer table is loaded from GeoJSON.
     */
    @Bean
    public PortDistanceProvider portDistanceProvider() {
        return new RandomPortDistanceProvider();
    }
}
