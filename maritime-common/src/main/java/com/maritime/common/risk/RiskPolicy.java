package com.maritime.common.risk;

/**
 * Single source of truth for risk-scoring weights, thresholds, and the label
 * classification logic shared across the enrichment pipeline.
 *
 * <p>Both {@code RiskScorerEnrichService} (AIS event path) and
 * {@code HexCrossingEnricherService} (hex-crossing path) apply the same model.
 * Centralising the constants here ensures a single change propagates to both
 * consumers, preventing silent divergence in risk labels produced by the same
 * pipeline.
 */
public final class RiskPolicy {

    // ── Zone weights ──────────────────────────────────────────────────────────
    public static final double RESTRICTED_ZONE_WEIGHT = 50.0;
    public static final double PORT_ZONE_WEIGHT        = 20.0;
    public static final double EEZ_ZONE_WEIGHT         = 10.0;

    // ── Proximity / behaviour weights ─────────────────────────────────────────
    public static final double NEAR_PORT_WEIGHT        = 20.0;
    public static final double HIGH_SPEED_WEIGHT       = 10.0;

    // ── Decision thresholds ───────────────────────────────────────────────────
    public static final double NEAR_PORT_THRESHOLD_NM  = 10.0;
    public static final double HIGH_SPEED_THRESHOLD_KN = 25.0;
    public static final double HIGH_RISK_THRESHOLD     = 50.0;
    public static final double MEDIUM_RISK_THRESHOLD   = 20.0;

    private RiskPolicy() {}

    /**
     * Converts a numeric risk score to its categorical label.
     *
     * @param score accumulated risk score
     * @return {@code "HIGH"}, {@code "MEDIUM"}, or {@code "LOW"}
     */
    public static String toRiskLevel(double score) {
        if (score >= HIGH_RISK_THRESHOLD)   return "HIGH";
        if (score >= MEDIUM_RISK_THRESHOLD) return "MEDIUM";
        return "LOW";
    }
}
