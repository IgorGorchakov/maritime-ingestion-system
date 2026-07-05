package com.maritime.detection.streams;

import java.time.Instant;

/**
 * Snapshot of a vessel's last known position, stored per-MMSI in the
 * Kafka Streams RocksDB state store.
 *
 * Serialized to/from JSON by {@link VesselStateSerdes} for the changelog topic.
 * Holds both the minimal detector state and the enrichment context so the
 * dark-vessel punctuator can reconstruct a complete {@code EnrichedVesselEvent}
 * when it emits to {@code maritime.detections}.
 *
 * <p>Adding fields is backward-safe: Jackson defaults absent fields on
 * deserialization, so existing changelog entries simply read with the new
 * fields unset (null / 0 / false).
 */
public class VesselState {

    private String  mmsi;
    private double  latitude;
    private double  longitude;
    private double  speed;          // knots, reported SOG
    private long    lastSeenMs;     // epoch millis — used by the dark-vessel punctuator
    private boolean loitering;
    private boolean darkVessel;
    private boolean speedAnomaly;

    // Enrichment context — retained so the dark-vessel punctuator can rebuild
    // a complete EnrichedVesselEvent without access to the original record.
    private boolean inRestrictedZone;
    private String  zoneName;
    private String  zoneType;
    private double  distanceToPort;
    private double  riskScore;
    private String  riskLevel;

    /** Required by Jackson for deserialization. */
    public VesselState() {}

    public VesselState(String mmsi, double latitude, double longitude,
                       double speed, long lastSeenMs) {
        this.mmsi        = mmsi;
        this.latitude    = latitude;
        this.longitude   = longitude;
        this.speed       = speed;
        this.lastSeenMs  = lastSeenMs;
        this.loitering   = false;
        this.darkVessel  = false;
        this.speedAnomaly = false;
    }

    public String  getMmsi()                      { return mmsi; }
    public void    setMmsi(String mmsi)           { this.mmsi = mmsi; }
    public double  getLatitude()                  { return latitude; }
    public void    setLatitude(double v)          { this.latitude = v; }
    public double  getLongitude()                 { return longitude; }
    public void    setLongitude(double v)         { this.longitude = v; }
    public double  getSpeed()                     { return speed; }
    public void    setSpeed(double v)             { this.speed = v; }
    public long    getLastSeenMs()                { return lastSeenMs; }
    public void    setLastSeenMs(long v)          { this.lastSeenMs = v; }
    public boolean isLoitering()                  { return loitering; }
    public void    setLoitering(boolean v)        { this.loitering = v; }
    public boolean isDarkVessel()                 { return darkVessel; }
    public void    setDarkVessel(boolean v)       { this.darkVessel = v; }
    public boolean isSpeedAnomaly()               { return speedAnomaly; }
    public void    setSpeedAnomaly(boolean v)     { this.speedAnomaly = v; }
    public boolean isInRestrictedZone()           { return inRestrictedZone; }
    public void    setInRestrictedZone(boolean v) { this.inRestrictedZone = v; }
    public String  getZoneName()                  { return zoneName; }
    public void    setZoneName(String v)          { this.zoneName = v; }
    public String  getZoneType()                  { return zoneType; }
    public void    setZoneType(String v)          { this.zoneType = v; }
    public double  getDistanceToPort()            { return distanceToPort; }
    public void    setDistanceToPort(double v)    { this.distanceToPort = v; }
    public double  getRiskScore()                 { return riskScore; }
    public void    setRiskScore(double v)         { this.riskScore = v; }
    public String  getRiskLevel()                 { return riskLevel; }
    public void    setRiskLevel(String v)         { this.riskLevel = v; }
    public Instant lastSeen()                     { return Instant.ofEpochMilli(lastSeenMs); }

    @Override
    public String toString() {
        return "VesselState{mmsi=" + mmsi + ", lat=" + latitude + ", lon=" + longitude
                + ", speed=" + speed + ", lastSeenMs=" + lastSeenMs
                + ", loitering=" + loitering + ", dark=" + darkVessel
                + ", speedAnomaly=" + speedAnomaly
                + ", zone=" + zoneName + ", risk=" + riskLevel + "}";
    }
}
