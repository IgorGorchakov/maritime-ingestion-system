package com.maritime.enricher.service;

/**
 * Strategy interface for computing a vessel's distance to the nearest known port.
 *
 * <h3>Why an interface?</h3>
 * Distance-to-port is used in risk scoring but its data source is not yet
 * finalised. Hiding the implementation behind this contract means:
 * <ul>
 *   <li>The risk scorer is testable today — unit tests inject a deterministic
 *       stub without any I/O or randomness.</li>
 *   <li>The real implementation can be swapped in (Phase 7: PostGIS
 *       {@code ST_Distance} against a port gazetteer loaded from
 *       {@code infra/data/ports.geojson}) without touching call sites.</li>
 *   <li>Future alternatives (e.g. a cached in-memory KD-tree for ultra-low
 *       latency, or a remote port-lookup microservice) are plug-in replacements.</li>
 * </ul>
 *
 * <h3>Contract</h3>
 * <ul>
 *   <li>Returns distance in nautical miles (NM), consistent with the AIS speed
 *       unit (knots) used elsewhere in the platform.</li>
 *   <li>Must never return a negative value.</li>
 *   <li>Implementations are expected to be thread-safe — the provider is a
 *       singleton Spring bean shared across all Kafka listener threads.</li>
 * </ul>
 *
 * @see RandomPortDistanceProvider the current placeholder implementation
 */
public interface PortDistanceProvider {

    /**
     * Return the distance in nautical miles from the given position to the
     * nearest known port.
     *
     * @param latitudeDeg  WGS-84 latitude in decimal degrees  [-90, 90]
     * @param longitudeDeg WGS-84 longitude in decimal degrees [-180, 180]
     * @return distance in nautical miles, &ge; 0
     */
    double distanceToNearestPortNm(double latitudeDeg, double longitudeDeg);
}
