package com.maritime.streaming.streams;

import java.time.Instant;

/**
 * Immutable snapshot of a vessel's last known position, stored in the
 * Kafka Streams KTable (RocksDB state store) keyed by MMSI.
 *
 * Serialized to/from JSON by {@link VesselStateSerdes} for the changelog topic.
 * Fields are deliberately minimal — only what the detectors need.
 */
public class VesselState {

    private String mmsi;
    private double latitude;
    private double longitude;
    private double speed;         // knots, reported SOG
    private long   lastSeenMs;    // epoch millis — used by dark-vessel punctuator
    private boolean loitering;
    private boolean darkVessel;
    private boolean speedAnomaly;

    // Required by Jackson for deserialization
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

    // ── Getters / setters ────────────────────────────────────────────────────

    public String  getMmsi()        { return mmsi; }
    public double  getLatitude()    { return latitude; }
    public double  getLongitude()   { return longitude; }
    public double  getSpeed()       { return speed; }
    public long    getLastSeenMs()  { return lastSeenMs; }
    public boolean isLoitering()    { return loitering; }
    public boolean isDarkVessel()   { return darkVessel; }
    public boolean isSpeedAnomaly() { return speedAnomaly; }

    public void setMmsi(String mmsi)              { this.mmsi = mmsi; }
    public void setLatitude(double latitude)      { this.latitude = latitude; }
    public void setLongitude(double longitude)    { this.longitude = longitude; }
    public void setSpeed(double speed)            { this.speed = speed; }
    public void setLastSeenMs(long lastSeenMs)    { this.lastSeenMs = lastSeenMs; }
    public void setLoitering(boolean loitering)   { this.loitering = loitering; }
    public void setDarkVessel(boolean darkVessel) { this.darkVessel = darkVessel; }
    public void setSpeedAnomaly(boolean v)        { this.speedAnomaly = v; }

    public Instant lastSeen() { return Instant.ofEpochMilli(lastSeenMs); }

    @Override
    public String toString() {
        return "VesselState{mmsi=" + mmsi + ", lat=" + latitude + ", lon=" + longitude
                + ", speed=" + speed + ", lastSeenMs=" + lastSeenMs
                + ", loitering=" + loitering + ", dark=" + darkVessel
                + ", speedAnomaly=" + speedAnomaly + "}";
    }
}
