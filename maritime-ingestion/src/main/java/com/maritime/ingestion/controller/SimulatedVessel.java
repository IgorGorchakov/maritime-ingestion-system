package com.maritime.ingestion.controller;

/**
 * Catalogue of simulated vessels used by {@link AisSimulatorController}.
 *
 * <p>Each constant bundles everything the simulator needs for one vessel:
 * MMSI, display label, waypoint track, speed, and heading. Adding or removing
 * a vessel from the fleet requires only touching this file.
 *
 * <h3>Vessel roles</h3>
 * <ul>
 *   <li>{@link #NORMAL_TRANSIT}   — baseline; exercises normal enrichment path</li>
 *   <li>{@link #LOITERER}         — slow speed in a tight box; fires loitering detector</li>
 *   <li>{@link #DARK_VESSEL}      — goes silent after 12 ticks; fires dark-vessel detector</li>
 *   <li>{@link #SPEED_ANOMALY}    — low reported SOG, huge position jump; fires speed detector</li>
 *   <li>{@link #TANKER_ALPHA}     — southbound Texas → Yucatan</li>
 *   <li>{@link #TANKER_BRAVO}     — westbound Florida → Texas</li>
 *   <li>{@link #CARGO_ALPHA}      — northbound Cuba → New Orleans</li>
 *   <li>{@link #CARGO_BRAVO}      — deep-gulf east-to-west transit</li>
 *   <li>{@link #FISHING_VESSEL}   — slow meander near Louisiana coast</li>
 * </ul>
 */
public enum SimulatedVessel {

    NORMAL_TRANSIT("123456789", "Normal Transit", 12.0, 90.0, new double[][]{
        {30.5, -90.0}, {30.6, -89.5}, {30.7, -89.0}, {30.8, -88.5},
        {30.9, -88.0}, {31.0, -87.5}, {31.1, -87.0}, {31.2, -86.5},
        {31.3, -86.0}, {31.4, -85.5}, {31.5, -85.0}, {31.6, -84.5}
    }),

    LOITERER("234567890", "Loiterer", 0.3, 0.0, null),       // uses random drift, not waypoints

    DARK_VESSEL("345678901", "Dark Vessel", 8.0, 45.0, null), // built per-tick, goes silent at tick 12

    SPEED_ANOMALY("456789012", "Speed Anomaly", 2.0, 180.0, null), // large position jump per tick

    TANKER_ALPHA("111111111", "Tanker Alpha", 14.0, 180.0, new double[][]{
        {29.5, -94.0}, {28.8, -93.5}, {28.0, -93.0}, {27.0, -92.5},
        {26.0, -92.0}, {25.0, -91.5}, {24.0, -91.0}, {23.0, -90.5},
        {22.5, -90.0}, {22.0, -89.5}, {21.5, -89.0}, {21.0, -88.5}
    }),

    TANKER_BRAVO("222222222", "Tanker Bravo", 13.5, 270.0, new double[][]{
        {25.5, -80.5}, {25.8, -82.0}, {26.0, -83.5}, {26.2, -85.0},
        {26.5, -86.5}, {26.8, -88.0}, {27.0, -89.5}, {27.2, -91.0},
        {27.5, -92.5}, {27.8, -93.5}, {28.0, -94.5}, {28.2, -95.5}
    }),

    CARGO_ALPHA("333333333", "Cargo Alpha", 11.0, 0.0, new double[][]{
        {23.5, -82.0}, {24.5, -83.0}, {25.5, -84.0}, {26.5, -85.0},
        {27.5, -86.0}, {28.5, -87.0}, {29.0, -88.0}, {29.5, -88.5},
        {29.8, -89.0}, {30.0, -89.3}, {30.2, -89.6}, {30.4, -89.9}
    }),

    CARGO_BRAVO("444444444", "Cargo Bravo", 12.5, 270.0, new double[][]{
        {27.0, -84.0}, {27.0, -85.5}, {27.0, -87.0}, {27.0, -88.5},
        {27.0, -90.0}, {27.0, -91.5}, {27.0, -93.0}, {27.0, -94.5},
        {26.5, -94.5}, {26.0, -93.5}, {26.0, -92.0}, {26.0, -90.0}
    }),

    FISHING_VESSEL("555555555", "Fishing Vessel", 3.5, 45.0, new double[][]{
        {29.0, -90.5}, {29.1, -90.2}, {29.3, -90.0}, {29.2, -89.8},
        {29.0, -89.6}, {28.9, -89.4}, {29.1, -89.2}, {29.3, -89.0},
        {29.4, -89.2}, {29.2, -89.5}, {29.1, -89.8}, {29.0, -90.2}
    });

    public final String   mmsi;
    public final String   label;
    public final double   speed;
    public final double   heading;
    /** Waypoint track; {@code null} for vessels that compute position ad-hoc. */
    public final double[][] waypoints;

    SimulatedVessel(String mmsi, String label, double speed, double heading, double[][] waypoints) {
        this.mmsi      = mmsi;
        this.label     = label;
        this.speed     = speed;
        this.heading   = heading;
        this.waypoints = waypoints;
    }

    /** Position at tick {@code t}, wrapping around the waypoint list. */
    public double[] positionAt(int t) {
        if (waypoints == null) throw new UnsupportedOperationException(name() + " has no fixed waypoints");
        return waypoints[t % waypoints.length];
    }
}
