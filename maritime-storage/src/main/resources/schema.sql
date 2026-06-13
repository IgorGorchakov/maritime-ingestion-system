-- Hot tier: latest known state per vessel, keyed by MMSI.
-- Owned by the storage service (run on startup via spring.sql.init.mode=always),
-- independent of streaming's Flyway migrations on the shared `maritime` database.
-- Replaces the former DynamoDB `vessel-risk` table.
CREATE TABLE IF NOT EXISTS vessel_risk (
    mmsi               VARCHAR(9)       PRIMARY KEY,
    risk_level         VARCHAR(16),
    risk_score         DOUBLE PRECISION,
    in_restricted_zone BOOLEAN,
    loitering          BOOLEAN,
    dark_vessel        BOOLEAN,
    speed_anomaly      BOOLEAN,
    zone_name          VARCHAR(128),
    zone_type          VARCHAR(32),
    -- Canonical Avro-JSON of the full EnrichedVesselEvent, returned verbatim by
    -- GET /api/v1/vessels/{mmsi} so the gateway HTTP contract is a byte-for-byte
    -- round-trip. The flat columns above stay queryable for ad-hoc SQL / dashboards.
    payload            JSONB            NOT NULL
);
