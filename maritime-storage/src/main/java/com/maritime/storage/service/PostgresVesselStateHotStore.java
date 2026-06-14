package com.maritime.storage.service;

import com.maritime.common.dto.EnrichedVesselEvent;
import com.maritime.common.serde.AvroJson;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Postgres-backed hot tier (replaces the previous DynamoDB implementation).
 *
 * <p>One row per vessel, keyed by MMSI. Writes are an {@code INSERT ... ON CONFLICT
 * (mmsi) DO UPDATE} upsert so the row always reflects the latest event — the same
 * "latest state per key" semantics DynamoDB's {@code putItem} gave us.
 *
 * <h3>Why a {@code payload} column alongside the flat columns?</h3>
 * The API service HTTP contract returns the full Avro-JSON of the enriched event. We
 * store that canonical JSON in {@code payload} (via {@link AvroJson}) so the GET
 * path is a byte-for-byte round-trip, while the flat columns
 * ({@code risk_level}, {@code loitering}, …) stay queryable for ad-hoc SQL and
 * Grafana/Postgres dashboards.
 */
@Slf4j
@Repository
public class PostgresVesselStateHotStore implements VesselStateStore {

    private final JdbcTemplate jdbc;

    public PostgresVesselStateHotStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static final String UPSERT_SQL = """
            INSERT INTO vessel_risk (
                mmsi, risk_level, risk_score, in_restricted_zone,
                loitering, dark_vessel, speed_anomaly, zone_name, zone_type, payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
            ON CONFLICT (mmsi) DO UPDATE SET
                risk_level         = EXCLUDED.risk_level,
                risk_score         = EXCLUDED.risk_score,
                in_restricted_zone = EXCLUDED.in_restricted_zone,
                loitering          = EXCLUDED.loitering,
                dark_vessel        = EXCLUDED.dark_vessel,
                speed_anomaly      = EXCLUDED.speed_anomaly,
                zone_name          = EXCLUDED.zone_name,
                zone_type          = EXCLUDED.zone_type,
                payload            = EXCLUDED.payload
            """;

    @Override
    public void upsert(EnrichedVesselEvent event) {
        String mmsi = event.getVesselEvent().getMmsi();
        jdbc.update(UPSERT_SQL,
                mmsi,
                event.getRiskLevel(),
                event.getRiskScore(),
                event.getInRestrictedZone(),
                event.getLoitering(),
                event.getDarkVessel(),
                event.getSpeedAnomaly(),
                event.getZoneName(),
                event.getZoneType(),
                AvroJson.toJson(event));
        log.debug("Upserted hot-tier state for MMSI={}", mmsi);
    }

    @Override
    public Optional<EnrichedVesselEvent> findByMmsi(String mmsi) {
        return jdbc.query(
                "SELECT payload FROM vessel_risk WHERE mmsi = ?",
                rs -> rs.next()
                        ? Optional.of(AvroJson.fromJson(rs.getString("payload"), EnrichedVesselEvent.class))
                        : Optional.empty(),
                mmsi);
    }
}
