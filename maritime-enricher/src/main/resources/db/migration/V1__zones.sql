-- Phase 6: PostGIS zone catalog
-- Enables the PostGIS extension (pre-installed in postgis/postgis:15-3.3 image),
-- creates the zones table with a GiST spatial index, and seeds representative zones
-- so the detectors fire without requiring a manual data load.

CREATE EXTENSION IF NOT EXISTS postgis;

-- zones: authoritative geofence catalog.
-- zone_type drives risk scoring: RESTRICTED > EEZ > PORT.
-- geometry column uses SRID 4326 (WGS-84 lat/lon) matching raw AIS coordinates.
CREATE TABLE IF NOT EXISTS zones (
    id          BIGSERIAL PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    zone_type   VARCHAR(50)  NOT NULL,   -- RESTRICTED | EEZ | PORT
    geometry    GEOMETRY(POLYGON, 4326) NOT NULL
);

-- GiST index: turns ST_Contains from a full table scan into a fast spatial lookup.
-- Essential once the catalog grows beyond a handful of zones.
CREATE INDEX IF NOT EXISTS zones_geometry_gist ON zones USING GIST (geometry);

-- ── Seed zones ────────────────────────────────────────────────────────────────
-- Gulf of Mexico restricted zone (overlaps the simulator default track area so
-- vessels passing through will trigger inRestrictedZone=true and zoneType=RESTRICTED).
INSERT INTO zones (name, zone_type, geometry) VALUES (
    'Gulf of Mexico Restricted Zone',
    'RESTRICTED',
    ST_GeomFromText(
        'POLYGON((-88.0 32.0, -88.0 35.0, -85.0 35.0, -85.0 32.0, -88.0 32.0))',
        4326
    )
);

-- New Orleans port approach zone (simulator vessels near -90 lon will enter this).
INSERT INTO zones (name, zone_type, geometry) VALUES (
    'Port of New Orleans',
    'PORT',
    ST_GeomFromText(
        'POLYGON((-90.5 29.5, -90.5 30.2, -89.5 30.2, -89.5 29.5, -90.5 29.5))',
        4326
    )
);

-- US Gulf Coast EEZ excerpt (large background zone).
INSERT INTO zones (name, zone_type, geometry) VALUES (
    'US Gulf Coast EEZ',
    'EEZ',
    ST_GeomFromText(
        'POLYGON((-97.0 25.0, -97.0 30.5, -80.0 30.5, -80.0 25.0, -97.0 25.0))',
        4326
    )
);
