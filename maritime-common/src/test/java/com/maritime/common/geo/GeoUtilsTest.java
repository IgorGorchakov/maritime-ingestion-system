package com.maritime.common.geo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit tests for {@link GeoUtils}.
 *
 * Covers Haversine distance accuracy and point-in-polygon correctness —
 * both are foundational to the zone-lookup and speed-anomaly detector.
 */
class GeoUtilsTest {

    // ── Haversine distance ────────────────────────────────────────────────────

    @Test
    void samePoint_returnsZero() {
        double dist = GeoUtils.calculateDistanceInMeters(51.5, -0.12, 51.5, -0.12);
        assertThat(dist).isEqualTo(0.0);
    }

    @Test
    void londonToParis_roughly343km() {
        // Great-circle distance London (51.507, -0.128) → Paris (48.857, 2.352).
        // Haversine on a 6 371 km sphere yields ≈ 343.5 km; assert within ±2 km.
        double dist = GeoUtils.calculateDistanceInMeters(51.507, -0.128, 48.857, 2.352);
        assertThat(dist).isCloseTo(343_500, within(2_000.0));  // ±2 km tolerance
    }

    @Test
    void antimeridianCrossing_doesNotOverflow() {
        // Fiji (−18, 178) to Samoa (−14, −172) — crosses the 180° antimeridian.
        // Haversine handles this correctly; verify the result is physically plausible.
        double dist = GeoUtils.calculateDistanceInMeters(-18, 178, -14, -172);
        assertThat(dist).isPositive().isLessThan(2_000_000); // < 2000 km
    }

    @ParameterizedTest(name = "({0},{1})→({2},{3}) ≥ 0")
    @CsvSource({
        "0,0,0,1",
        "90,-180,-90,180",
        "-33.87,151.21,35.69,139.69"  // Sydney → Tokyo
    })
    void distance_alwaysNonNegative(double lat1, double lon1, double lat2, double lon2) {
        assertThat(GeoUtils.calculateDistanceInMeters(lat1, lon1, lat2, lon2)).isGreaterThanOrEqualTo(0);
    }

    // ── Point-in-polygon ──────────────────────────────────────────────────────

    private static final java.util.List<double[]> GULF_BOX = java.util.List.of(
        new double[]{-90, 29}, new double[]{-89, 29},
        new double[]{-89, 30}, new double[]{-90, 30},
        new double[]{-90, 29}  // closed ring
    );

    @Test
    void pointInsidePolygon_returnsTrue() {
        // (-89.5, 29.5) is the centre of the Gulf box
        assertThat(GeoUtils.isPointInPolygon(29.5, -89.5, GULF_BOX)).isTrue();
    }

    @Test
    void pointOutsidePolygon_returnsFalse() {
        assertThat(GeoUtils.isPointInPolygon(35.0, -80.0, GULF_BOX)).isFalse();
    }

    @Test
    void nullOrEmptyPolygon_returnsFalse() {
        assertThat(GeoUtils.isPointInPolygon(29.5, -89.5, null)).isFalse();
        assertThat(GeoUtils.isPointInPolygon(29.5, -89.5, java.util.List.of())).isFalse();
    }
}
