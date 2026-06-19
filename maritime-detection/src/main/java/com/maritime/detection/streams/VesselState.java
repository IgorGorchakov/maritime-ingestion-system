package com.maritime.detection.streams;

import java.time.Instant;

/**
 * Snapshot of a vessel's last known position, stored per-MMSI in the
 * Kafka Streams RocksDB state store.
 *
 * Serialized to/from JSON by {@link VesselStateSerdes} for the changelog topic.
 * Fields are deliberately minimal — only what the three detectors need.
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
    public Instant lastSeen()                     { return Instant.ofEpochMilli(lastSeenMs); }

    @Override
    public String toString() {
        return "VesselState{mmsi=" + mmsi + ", lat=" + latitude + ", lon=" + longitude
                + ", speed=" + speed + ", lastSeenMs=" + lastSeenMs
                + ", loitering=" + loitering + ", dark=" + darkVessel
                + ", speedAnomaly=" + speedAnomaly + "}";
    }
}
