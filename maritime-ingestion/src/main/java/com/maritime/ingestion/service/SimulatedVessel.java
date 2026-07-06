package com.maritime.ingestion.service;

import com.maritime.common.kafka.Topics;

/**
 * Catalogue of simulated vessels used by {@link AisSimulatorService}.
 *
 * <p>Each constant bundles everything the simulator needs for one vessel:
 * MMSI, display label, speed, its motion model, and (for the dark vessel) when it
 * goes silent. Adding or removing a vessel from the fleet requires only touching
 * this file.
 *
 * <h3>Motion model</h3>
 * Every vessel supplies a total {@link Motion} function that yields its
 * {@code [latitude, longitude, heading]} at any tick — so {@link #motionAt(int)} is
 * defined for <em>all</em> vessels, with no partial operations. Two flavours exist:
 * <ul>
 *   <li><b>Waypoint vessels</b> interpolate linearly between consecutive waypoints
 *       over {@code ticksPerWaypoint} ticks (1 tick = 2 s), with heading taken from
 *       the bearing to the next waypoint so the marker rotates as the vessel turns.</li>
 *   <li><b>Ad-hoc vessels</b> ({@link #LOITERER}, {@link #DARK_VESSEL},
 *       {@link #SPEED_ANOMALY}) compute position procedurally to exercise a specific
 *       detector.</li>
 * </ul>
 *
 * <h3>Transmission</h3>
 * Motion is independent of transmission: a vessel always <em>has</em> a position, but
 * may choose not to broadcast. {@link #transmitsAt(int)} defaults to {@code true};
 * {@link #DARK_VESSEL} overrides it to fall silent after
 * {@link #DARK_VESSEL_SILENCE_AFTER} ticks so the dark-vessel detector fires.
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

    //                  mmsi         label               speed  ticks  waypoints           source
    // Coastal transit within VHF range → terrestrial receiver
    NORMAL_TRANSIT("123456789", "Normal Transit",        12.0,  10, new double[][]{
        {30.5, -90.0}, {30.6, -89.5}, {30.7, -89.0}, {30.8, -88.5},
        {30.9, -88.0}, {31.0, -87.5}, {31.1, -87.0}, {31.2, -86.5},
        {31.3, -86.0}, {31.4, -85.5}, {31.5, -85.0}, {31.6, -84.5}
    }, AisSource.TERRESTRIAL),

    // Drifting in port approaches → terrestrial receiver.
    // Smooth circular drift (~0.02° radius) centred near (-89.8, 30.1). Deterministic
    // sin/cos gives visible movement without jitter; speed is well below the 1-kn
    // loitering threshold. Heading is tangent to the circle.
    LOITERER("234567890", "Loiterer", 0.3, AisSource.TERRESTRIAL, t -> {
        double angle   = t * 0.2;   // radians; full circle every ~31 ticks (~62 s)
        double lat     = 30.10 + Math.sin(angle) * 0.02;
        double lon     = -89.80 + Math.cos(angle) * 0.02;
        double heading = (Math.toDegrees(angle + Math.PI / 2) + 360) % 360;
        return new double[]{lat, lon, heading};
    }),

    // Goes silent → last seen by vessel-to-vessel relay.
    // Slow north-east transit; stops transmitting after DARK_VESSEL_SILENCE_AFTER ticks
    // (see the transmitsAt override) so the dark-vessel punctuator fires.
    DARK_VESSEL("345678901", "Dark Vessel", 8.0, AisSource.VESSEL, t -> {
        double lat = 30.0 + t * 0.01;
        double lon = -89.5 + t * 0.02;
        return new double[]{lat, lon, 45.0};
    }) {
        @Override
        public boolean transmitsAt(int t) {
            return t < DARK_VESSEL_SILENCE_AFTER;
        }
    },

    // Satellite lag causes large implied-speed divergence from reported SOG.
    // Reports SOG = 2 kn but jumps ~0.4° (~24 nm) per 2-second tick; the detector flags
    // the divergence from implied speed on the second report.
    SPEED_ANOMALY("456789012", "Speed Anomaly", 2.0, AisSource.SATELLITE, t -> {
        double lat = 29.5 + (t % 10) * 0.4;
        double lon = -90.0 + (t % 6) * 0.4;
        return new double[]{lat, lon, 180.0};
    }),

    // Open-gulf, beyond VHF range → satellite
    TANKER_ALPHA("111111111", "Tanker Alpha",            14.0,  12, new double[][]{
        {29.5, -94.0}, {28.8, -93.5}, {28.0, -93.0}, {27.0, -92.5},
        {26.0, -92.0}, {25.0, -91.5}, {24.0, -91.0}, {23.0, -90.5},
        {22.5, -90.0}, {22.0, -89.5}, {21.5, -89.0}, {21.0, -88.5}
    }, AisSource.SATELLITE),

    // Open-gulf → satellite
    TANKER_BRAVO("222222222", "Tanker Bravo",            13.5,  12, new double[][]{
        {25.5, -80.5}, {25.8, -82.0}, {26.0, -83.5}, {26.2, -85.0},
        {26.5, -86.5}, {26.8, -88.0}, {27.0, -89.5}, {27.2, -91.0},
        {27.5, -92.5}, {27.8, -93.5}, {28.0, -94.5}, {28.2, -95.5}
    }, AisSource.SATELLITE),

    // Inshore route Cuba → New Orleans → terrestrial
    CARGO_ALPHA("333333333", "Cargo Alpha",              11.0,  10, new double[][]{
        {23.5, -82.0}, {24.5, -83.0}, {25.5, -84.0}, {26.5, -85.0},
        {27.5, -86.0}, {28.5, -87.0}, {29.0, -88.0}, {29.5, -88.5},
        {29.8, -89.0}, {30.0, -89.3}, {30.2, -89.6}, {30.4, -89.9}
    }, AisSource.TERRESTRIAL),

    // Deep-gulf east-to-west → tracked by passing vessels
    CARGO_BRAVO("444444444", "Cargo Bravo",              12.5,  10, new double[][]{
        {27.0, -84.0}, {27.0, -85.5}, {27.0, -87.0}, {27.0, -88.5},
        {27.0, -90.0}, {27.0, -91.5}, {27.0, -93.0}, {27.0, -94.5},
        {26.5, -94.5}, {26.0, -93.5}, {26.0, -92.0}, {26.0, -90.0}
    }, AisSource.VESSEL),

    // Near Louisiana coast → terrestrial
    FISHING_VESSEL("555555555", "Fishing Vessel",         3.5,   8, new double[][]{
        {29.0, -90.5}, {29.1, -90.2}, {29.3, -90.0}, {29.2, -89.8},
        {29.0, -89.6}, {28.9, -89.4}, {29.1, -89.2}, {29.3, -89.0},
        {29.4, -89.2}, {29.2, -89.5}, {29.1, -89.8}, {29.0, -90.2}
    }, AisSource.TERRESTRIAL);

    /** After this many ticks the dark vessel stops transmitting. */
    static final int DARK_VESSEL_SILENCE_AFTER = 12;

    /**
     * A vessel's motion model: position and heading at a given simulation tick.
     * Total by construction — every vessel supplies one, so {@link #motionAt(int)}
     * is defined for all constants.
     */
    @FunctionalInterface
    private interface Motion {
        /**
         * @param t simulation tick
         * @return three-element array {@code [latitude, longitude, headingDegrees]}
         */
        double[] at(int t);
    }

    /**
     * AIS reception channel — determines which raw topic this vessel's events
     * are published to. Each source type has independent scaling, retention,
     * and failure-handling characteristics.
     */
    public enum AisSource {
        /** Land/port-based VHF receiver — high update rate, short range. */
        TERRESTRIAL(Topics.AIS_RAW_TERRESTRIAL),
        /** Satellite AIS receiver — global coverage, lower update rate, potential staleness. */
        SATELLITE  (Topics.AIS_RAW_SATELLITE),
        /** AIS-to-AIS vessel relay — peer reports, variable reliability. */
        VESSEL     (Topics.AIS_RAW_VESSEL);

        public final String topic;
        AisSource(String topic) { this.topic = topic; }
    }

    public final String    mmsi;
    public final String    label;
    public final double    speed;
    /** Ticks to spend traversing each waypoint segment (0 for ad-hoc vessels). */
    public final int       ticksPerWaypoint;
    /** Waypoint track; {@code null} for ad-hoc vessels. */
    public final double[][]waypoints;
    /** AIS reception channel — determines the raw ingestion topic. */
    public final AisSource aisSource;
    /** Total motion function for this vessel — never {@code null}. */
    private final Motion   motion;

    /** Waypoint-driven vessel: motion is linear interpolation over the track. */
    SimulatedVessel(String mmsi, String label, double speed,
                    int ticksPerWaypoint, double[][] waypoints, AisSource aisSource) {
        this.mmsi             = mmsi;
        this.label            = label;
        this.speed            = speed;
        this.ticksPerWaypoint = ticksPerWaypoint;
        this.waypoints        = waypoints;
        this.aisSource        = aisSource;
        this.motion           = this::waypointMotion;
    }

    /** Ad-hoc vessel: motion is a procedural function; no waypoint track. */
    SimulatedVessel(String mmsi, String label, double speed,
                    AisSource aisSource, Motion motion) {
        this.mmsi             = mmsi;
        this.label            = label;
        this.speed            = speed;
        this.ticksPerWaypoint = 0;
        this.waypoints        = null;
        this.aisSource        = aisSource;
        this.motion           = motion;
    }

    /**
     * Position and heading at tick {@code t}. Defined for every vessel — waypoint
     * vessels interpolate along their track, ad-hoc vessels evaluate their procedural
     * motion.
     *
     * @return three-element array {@code [latitude, longitude, headingDegrees]}
     */
    public double[] motionAt(int t) {
        return motion.at(t);
    }

    /**
     * Whether this vessel broadcasts an AIS report at tick {@code t}. Defaults to
     * always transmitting; {@link #DARK_VESSEL} overrides this to go silent so the
     * dark-vessel detector fires.
     */
    public boolean transmitsAt(int t) {
        return true;
    }

    /**
     * Interpolated position and bearing at tick {@code t} for a waypoint vessel.
     *
     * <p>Within each segment the vessel moves linearly from {@code waypoints[i]} to
     * {@code waypoints[i+1]} over {@code ticksPerWaypoint} ticks, producing smooth
     * continuous movement instead of per-tick jumps. Heading is the bearing from the
     * current waypoint to the next.
     */
    private double[] waypointMotion(int t) {
        int total     = waypoints.length * ticksPerWaypoint;
        int wrapped   = t % total;
        int segIndex  = wrapped / ticksPerWaypoint;
        int nextIndex = (segIndex + 1) % waypoints.length;
        double frac   = (wrapped % ticksPerWaypoint) / (double) ticksPerWaypoint;

        double lat = waypoints[segIndex][0] + (waypoints[nextIndex][0] - waypoints[segIndex][0]) * frac;
        double lon = waypoints[segIndex][1] + (waypoints[nextIndex][1] - waypoints[segIndex][1]) * frac;

        return new double[]{lat, lon, bearing(waypoints[segIndex], waypoints[nextIndex])};
    }

    /** Bearing (degrees, 0=N clockwise) from waypoint {@code from} to {@code to}. */
    private static double bearing(double[] from, double[] to) {
        double lat1 = Math.toRadians(from[0]);
        double lon1 = Math.toRadians(from[1]);
        double lat2 = Math.toRadians(to[0]);
        double lon2 = Math.toRadians(to[1]);

        double dLon  = lon2 - lon1;
        double y     = Math.sin(dLon) * Math.cos(lat2);
        double x     = Math.cos(lat1) * Math.sin(lat2) - Math.sin(lat1) * Math.cos(lat2) * Math.cos(dLon);
        double angle = Math.toDegrees(Math.atan2(y, x));
        return (angle + 360) % 360;
    }
}
