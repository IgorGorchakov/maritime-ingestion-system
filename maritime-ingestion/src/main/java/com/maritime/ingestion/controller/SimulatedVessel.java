package com.maritime.ingestion.controller;

/**
 * Catalogue of simulated vessels used by {@link AisSimulatorController}.
 *
 * <p>Each constant bundles everything the simulator needs for one vessel:
 * MMSI, display label, speed, ticks-per-waypoint, and the waypoint track.
 * Adding or removing a vessel from the fleet requires only touching this file.
 *
 * <h3>Motion model</h3>
 * Vessels with waypoints are linearly interpolated between consecutive waypoints
 * over {@code ticksPerWaypoint} ticks (1 tick = 2 s). This produces smooth,
 * continuous movement rather than jumping from point to point every event.
 * Heading is computed from the bearing between the current and next waypoint,
 * so the triangle marker rotates naturally as the vessel turns.
 *
 * <h3>Vessel roles</h3>
 * <ul>
 *   <li>{@link #NORMAL_TRANSIT}  — baseline; crosses the RESTRICTED zone</li>
 *   <li>{@link #LOITERER}        — circular drift; fires loitering detector</li>
 *   <li>{@link #DARK_VESSEL}     — goes silent after 12 ticks; fires dark-vessel detector</li>
 *   <li>{@link #SPEED_ANOMALY}   — large position jumps; fires speed-anomaly detector</li>
 *   <li>{@link #TANKER_ALPHA}    — southbound Texas → Yucatan</li>
 *   <li>{@link #TANKER_BRAVO}    — westbound Florida → Texas</li>
 *   <li>{@link #CARGO_ALPHA}     — northbound Cuba → New Orleans</li>
 *   <li>{@link #CARGO_BRAVO}     — deep-gulf east-to-west transit</li>
 *   <li>{@link #FISHING_VESSEL}  — slow meander near Louisiana coast</li>
 * </ul>
 */
public enum SimulatedVessel {

    //                  mmsi         label               speed  ticks  waypoints
    NORMAL_TRANSIT("123456789", "Normal Transit",        12.0,  10, new double[][]{
        {30.5, -90.0}, {30.6, -89.5}, {30.7, -89.0}, {30.8, -88.5},
        {30.9, -88.0}, {31.0, -87.5}, {31.1, -87.0}, {31.2, -86.5},
        {31.3, -86.0}, {31.4, -85.5}, {31.5, -85.0}, {31.6, -84.5}
    }),

    LOITERER("234567890", "Loiterer",                    0.3,   0, null), // circular drift in controller

    DARK_VESSEL("345678901", "Dark Vessel",               8.0,   0, null), // per-tick, goes silent at tick 12

    SPEED_ANOMALY("456789012", "Speed Anomaly",           2.0,   0, null), // large position jump per tick

    TANKER_ALPHA("111111111", "Tanker Alpha",            14.0,  12, new double[][]{
        {29.5, -94.0}, {28.8, -93.5}, {28.0, -93.0}, {27.0, -92.5},
        {26.0, -92.0}, {25.0, -91.5}, {24.0, -91.0}, {23.0, -90.5},
        {22.5, -90.0}, {22.0, -89.5}, {21.5, -89.0}, {21.0, -88.5}
    }),

    TANKER_BRAVO("222222222", "Tanker Bravo",            13.5,  12, new double[][]{
        {25.5, -80.5}, {25.8, -82.0}, {26.0, -83.5}, {26.2, -85.0},
        {26.5, -86.5}, {26.8, -88.0}, {27.0, -89.5}, {27.2, -91.0},
        {27.5, -92.5}, {27.8, -93.5}, {28.0, -94.5}, {28.2, -95.5}
    }),

    CARGO_ALPHA("333333333", "Cargo Alpha",              11.0,  10, new double[][]{
        {23.5, -82.0}, {24.5, -83.0}, {25.5, -84.0}, {26.5, -85.0},
        {27.5, -86.0}, {28.5, -87.0}, {29.0, -88.0}, {29.5, -88.5},
        {29.8, -89.0}, {30.0, -89.3}, {30.2, -89.6}, {30.4, -89.9}
    }),

    CARGO_BRAVO("444444444", "Cargo Bravo",              12.5,  10, new double[][]{
        {27.0, -84.0}, {27.0, -85.5}, {27.0, -87.0}, {27.0, -88.5},
        {27.0, -90.0}, {27.0, -91.5}, {27.0, -93.0}, {27.0, -94.5},
        {26.5, -94.5}, {26.0, -93.5}, {26.0, -92.0}, {26.0, -90.0}
    }),

    FISHING_VESSEL("555555555", "Fishing Vessel",         3.5,   8, new double[][]{
        {29.0, -90.5}, {29.1, -90.2}, {29.3, -90.0}, {29.2, -89.8},
        {29.0, -89.6}, {28.9, -89.4}, {29.1, -89.2}, {29.3, -89.0},
        {29.4, -89.2}, {29.2, -89.5}, {29.1, -89.8}, {29.0, -90.2}
    });

    public final String    mmsi;
    public final String    label;
    public final double    speed;
    /** Ticks to spend traversing each waypoint segment (0 = ad-hoc position). */
    public final int       ticksPerWaypoint;
    /** Waypoint track; {@code null} for vessels with ad-hoc position logic. */
    public final double[][]waypoints;

    SimulatedVessel(String mmsi, String label, double speed,
                    int ticksPerWaypoint, double[][] waypoints) {
        this.mmsi             = mmsi;
        this.label            = label;
        this.speed            = speed;
        this.ticksPerWaypoint = ticksPerWaypoint;
        this.waypoints        = waypoints;
    }

    /**
     * Interpolated position at tick {@code t}.
     *
     * <p>Within each segment the vessel moves linearly from {@code waypoints[i]}
     * to {@code waypoints[i+1]} over {@code ticksPerWaypoint} ticks, producing
     * smooth continuous movement instead of per-tick jumps.
     */
    public double[] positionAt(int t) {
        if (waypoints == null || ticksPerWaypoint == 0) {
            throw new UnsupportedOperationException(name() + " has no fixed waypoints");
        }
        int total      = waypoints.length * ticksPerWaypoint;
        int wrapped    = t % total;
        int segIndex   = wrapped / ticksPerWaypoint;
        int nextIndex  = (segIndex + 1) % waypoints.length;
        double frac    = (wrapped % ticksPerWaypoint) / (double) ticksPerWaypoint;

        double lat = waypoints[segIndex][0] + (waypoints[nextIndex][0] - waypoints[segIndex][0]) * frac;
        double lon = waypoints[segIndex][1] + (waypoints[nextIndex][1] - waypoints[segIndex][1]) * frac;
        return new double[]{lat, lon};
    }

    /**
     * Bearing (degrees, 0=N clockwise) from current waypoint to the next,
     * suitable for the AIS heading field.
     */
    public double headingAt(int t) {
        if (waypoints == null || ticksPerWaypoint == 0) return 0.0;
        int total     = waypoints.length * ticksPerWaypoint;
        int segIndex  = (t % total) / ticksPerWaypoint;
        int nextIndex = (segIndex + 1) % waypoints.length;

        double lat1 = Math.toRadians(waypoints[segIndex][0]);
        double lon1 = Math.toRadians(waypoints[segIndex][1]);
        double lat2 = Math.toRadians(waypoints[nextIndex][0]);
        double lon2 = Math.toRadians(waypoints[nextIndex][1]);

        double dLon  = lon2 - lon1;
        double y     = Math.sin(dLon) * Math.cos(lat2);
        double x     = Math.cos(lat1) * Math.sin(lat2) - Math.sin(lat1) * Math.cos(lat2) * Math.cos(dLon);
        double angle = Math.toDegrees(Math.atan2(y, x));
        return (angle + 360) % 360;
    }
}
