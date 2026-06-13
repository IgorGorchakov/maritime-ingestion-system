package com.maritime.streaming.config;

import com.maritime.common.validation.VesselEventValidator;
import com.maritime.streaming.service.DedupService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Wires the Phase 3 validate/dedup pipeline collaborators as Spring beans.
 *
 * Both {@link VesselEventValidator} (framework-free, lives in maritime-common) and
 * {@link DedupService} are plain classes by design, so they are registered here
 * via @Bean rather than component-scanned.
 */
@Configuration
public class PipelineConfig {

    /**
     * Dedup window: a (mmsi, timestamp) key is treated as a duplicate if seen again
     * within this TTL. Defaults to 1 hour; override with dedup.ttl-minutes.
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
}
