package com.maritime.streaming.geo;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

/**
 * Spatial zone lookups backed by PostGIS.
 *
 * <h3>Query design</h3>
 * We select only {@code id}, {@code name}, {@code zone_type} — deliberately
 * excluding the {@code geometry} column. This avoids asking Hibernate to
 * deserialize WKB bytes into a Java type we don't use. The spatial predicate
 * ({@code ST_Contains}) runs entirely in the database.
 *
 * <h3>Argument order</h3>
 * {@code ST_MakePoint(x, y)} = {@code ST_MakePoint(longitude, latitude)} —
 * the GIS convention of x=easting, y=northing, which is the *opposite* of
 * human-readable (lat, lon). Parameters are named to make this explicit.
 *
 * <h3>Performance</h3>
 * The GiST index on {@code zones.geometry} (created in V1__zones.sql) makes
 * {@code ST_Contains} O(log n) rather than a full table scan.
 */
public interface ZoneRepository extends JpaRepository<Zone, Long> {

    @Query(value = """
            SELECT z.id, z.name, z.zone_type
            FROM   zones z
            WHERE  ST_Contains(z.geometry,
                       ST_SetSRID(ST_MakePoint(:lon, :lat), 4326))
            """, nativeQuery = true)
    List<ZoneView> findZonesContaining(@Param("lat") double lat, @Param("lon") double lon);

    /** Projection: only the columns we actually use from a zone lookup. */
    interface ZoneView {
        Long   getId();
        String getName();
        String getZone_type();   // snake_case matches the native SQL column alias
    }
}
