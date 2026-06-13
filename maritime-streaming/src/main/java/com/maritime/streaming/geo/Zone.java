package com.maritime.streaming.geo;

import jakarta.persistence.*;

/**
 * JPA entity mapping the {@code zones} table managed by Flyway V1__zones.sql.
 *
 * The {@code geometry} column is PostGIS GEOMETRY(POLYGON, 4326). We declare it
 * as a {@code byte[]} here because:
 * <ul>
 *   <li>Hibernate spatial (hibernate-spatial) is not on the classpath — we don't
 *       need to manipulate the geometry in Java; only the DB does (ST_Contains).</li>
 *   <li>PostgreSQL returns PostGIS geometry as WKB bytes over JDBC. Storing as
 *       {@code byte[]} lets Hibernate read the column without a custom type.</li>
 *   <li>The repository query selects only id/name/zone_type via a projection, so
 *       this field is never populated from query results.</li>
 * </ul>
 */
@Entity
@Table(name = "zones")
public class Zone {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String name;

    @Column(name = "zone_type", nullable = false)
    private String zoneType;   // RESTRICTED | EEZ | PORT

    // PostGIS geometry stored as raw WKB bytes — never read by Java code,
    // only used by ST_Contains in native SQL queries.
    @Column(columnDefinition = "geometry")
    private byte[] geometry;

    // Required by JPA
    protected Zone() {}

    public Long   getId()       { return id; }
    public String getName()     { return name; }
    public String getZoneType() { return zoneType; }
}
