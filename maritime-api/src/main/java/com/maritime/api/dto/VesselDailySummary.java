package com.maritime.api.dto;

/**
 * One day of Spark-computed batch history for a vessel, as returned by
 * {@code GET /api/v1/intelligence/{mmsi}/history}.
 *
 * <p>This is the typed contract that replaces the previous untyped
 * {@code Map<String, Object>} rows: the JSON field names below are the API's public
 * contract with the frontend, and a column rename in the query now fails at compile
 * time here rather than silently at runtime.
 *
 * <p>{@code p50Risk} and {@code p95Risk} are nullable — {@code RiskRollupJob} may not
 * have produced percentile rows for a given date, in which case the LEFT JOIN yields
 * SQL {@code NULL} and these fields are {@code null}.
 */
public record VesselDailySummary(
        String date,
        long   eventCount,
        double avgSpeedKn,
        double maxSpeedKn,
        double avgRiskScore,
        long   restrictedCount,
        long   loiteringCount,
        long   darkVesselCount,
        long   speedAnomalyCount,
        Double p50Risk,
        Double p95Risk
) {}
