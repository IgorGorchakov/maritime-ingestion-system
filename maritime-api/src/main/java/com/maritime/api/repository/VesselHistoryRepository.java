package com.maritime.api.repository;

import com.maritime.api.dto.VesselDailySummary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Read-only access to the Spark batch-rollup tables in PostGIS. Owns the
 * {@link JdbcTemplate} and the history SQL, and maps rows to the typed
 * {@link VesselDailySummary}.
 *
 * <p>Isolating the query here means it can be unit-tested with a mock
 * {@code JdbcTemplate} or an embedded database, with no Spring MVC involved, and the
 * HTTP layer no longer depends on {@code JdbcTemplate} or SQL (audit C2).
 */
@Repository
public class VesselHistoryRepository {

    /** Days of daily history returned by {@link #findHistory(String)}. */
    private static final int HISTORY_DAYS = 90;

    // LEFT JOIN vessel_risk_percentiles so rows with no percentile data
    // (e.g. only DailyVesselAggregatesJob ran, not RiskRollupJob) still appear.
    private static final String HISTORY_SQL = """
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
            LIMIT ?
            """;

    private final JdbcTemplate jdbc;

    public VesselHistoryRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Daily batch history for a vessel, ordered most-recent-first and capped at
     * {@value #HISTORY_DAYS} days. Returns an empty list when the Spark jobs have not
     * produced any rows for the vessel yet.
     */
    public List<VesselDailySummary> findHistory(String mmsi) {
        return jdbc.query(HISTORY_SQL, this::mapRow, mmsi, HISTORY_DAYS);
    }

    private VesselDailySummary mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new VesselDailySummary(
                rs.getString("date"),
                rs.getLong("eventCount"),
                rs.getDouble("avgSpeedKn"),
                rs.getDouble("maxSpeedKn"),
                rs.getDouble("avgRiskScore"),
                rs.getLong("restrictedCount"),
                rs.getLong("loiteringCount"),
                rs.getLong("darkVesselCount"),
                rs.getLong("speedAnomalyCount"),
                nullableDouble(rs, "p50Risk"),
                nullableDouble(rs, "p95Risk"));
    }

    /**
     * Reads a possibly-{@code NULL} numeric column as a {@link Double}. Unlike
     * {@link ResultSet#getDouble(String)} (which maps SQL {@code NULL} to {@code 0.0}),
     * this preserves {@code null} so an absent percentile is not reported as a real zero.
     */
    private static Double nullableDouble(ResultSet rs, String column) throws SQLException {
        double value = rs.getDouble(column);
        return rs.wasNull() ? null : value;
    }
}
