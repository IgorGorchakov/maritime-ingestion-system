-- Phase 7: Spark batch layer output tables
-- Created by Flyway (managed in maritime-streaming alongside V1__zones.sql).
-- These tables are written by the Spark jobs and read by the gateway history endpoint.

-- ── vessel_daily_stats ────────────────────────────────────────────────────────
-- One row per (mmsi, date). Written by DailyVesselAggregatesJob.
-- Primary key is (mmsi, date) so re-running the job for the same date is
-- idempotent (JDBC SaveMode.Overwrite + truncate=true handles the upsert).
CREATE TABLE IF NOT EXISTS vessel_daily_stats (
    mmsi                VARCHAR(9)   NOT NULL,
    date                DATE         NOT NULL,
    event_count         BIGINT       NOT NULL DEFAULT 0,
    avg_speed_kn        DOUBLE PRECISION,
    max_speed_kn        DOUBLE PRECISION,
    avg_risk_score      DOUBLE PRECISION,
    restricted_count    BIGINT       NOT NULL DEFAULT 0,
    loitering_count     BIGINT       NOT NULL DEFAULT 0,
    dark_vessel_count   BIGINT       NOT NULL DEFAULT 0,
    speed_anomaly_count BIGINT       NOT NULL DEFAULT 0,
    PRIMARY KEY (mmsi, date)
);

-- Index on date alone: the gateway history query filters on mmsi first (PK prefix
-- covers it) but the Grafana fleet-wide dashboard queries by date range only.
CREATE INDEX IF NOT EXISTS vessel_daily_stats_date_idx
    ON vessel_daily_stats (date DESC);

-- ── vessel_risk_percentiles ───────────────────────────────────────────────────
-- p50 / p95 risk score per vessel per day. Written by RiskRollupJob.
CREATE TABLE IF NOT EXISTS vessel_risk_percentiles (
    mmsi         VARCHAR(9)       NOT NULL,
    date         DATE             NOT NULL,
    p50_risk     DOUBLE PRECISION NOT NULL DEFAULT 0,
    p95_risk     DOUBLE PRECISION NOT NULL DEFAULT 0,
    sample_count BIGINT           NOT NULL DEFAULT 0,
    PRIMARY KEY (mmsi, date)
);

-- ── loitering_hotspots ────────────────────────────────────────────────────────
-- Top-50 grid cells with highest loitering event count per day.
-- Written by LoiteringHotspotJob.
CREATE TABLE IF NOT EXISTS loitering_hotspots (
    date         DATE             NOT NULL,
    cell_lat     DOUBLE PRECISION NOT NULL,
    cell_lon     DOUBLE PRECISION NOT NULL,
    event_count  BIGINT           NOT NULL DEFAULT 0,
    vessel_count BIGINT           NOT NULL DEFAULT 0,
    PRIMARY KEY (date, cell_lat, cell_lon)
);

-- Spatial index on the grid cell centre for PostGIS proximity queries.
-- Phase 8 MCP tool (list_loitering_hotspots) will use ST_DWithin against this.
CREATE INDEX IF NOT EXISTS loitering_hotspots_geom_idx
    ON loitering_hotspots
    USING GIST (ST_MakePoint(cell_lon, cell_lat)::geometry);
