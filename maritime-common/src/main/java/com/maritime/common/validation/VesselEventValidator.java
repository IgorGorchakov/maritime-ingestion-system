package com.maritime.common.validation;

import com.maritime.common.dto.VesselEvent;

import java.time.Instant;
import java.util.regex.Pattern;

/**
 * Pure validation logic for raw VesselEvent records.
 *
 * Checks:
 *  - MMSI: exactly 9 digits
 *  - Latitude: [-90, 90]
 *  - Longitude: [-180, 180]
 *  - Null island: not (0, 0)
 *  - Timestamp: not older than 24 hours from now
 *  - Speed: <= 102.2 knots (max possible for any vessel)
 *
 * Pure, stateless, unit-testable — no Spring or Kafka dependencies.
 */
public class VesselEventValidator {

    private static final Pattern MMSI_PATTERN = Pattern.compile("^\\d{9}$");
    private static final long MAX_AGE_MS = 24 * 60 * 60 * 1000L; // 24 hours
    private static final double MAX_SPEED_KNOTS = 102.2; // theoretical max (world record ~70+ kn, 102.2 is generous)

    /**
     * Validate a vessel event. Returns ValidationResult indicating validity and optional reason.
     */
    public ValidationResult validate(VesselEvent event) {
        // MMSI check: must be exactly 9 digits
        String mmsi = event.getMmsi();
        if (mmsi == null || !MMSI_PATTERN.matcher(mmsi).matches()) {
            return ValidationResult.invalid("invalid MMSI: " + mmsi);
        }

        // Latitude check
        double lat = event.getLatitude();
        if (lat == 0.0 && event.getLongitude() == 0.0) {
            return ValidationResult.invalid("null island (0,0)");
        }
        if (lat < -90.0 || lat > 90.0) {
            return ValidationResult.invalid("latitude out of range: " + lat);
        }

        // Longitude check
        double lon = event.getLongitude();
        if (lon < -180.0 || lon > 180.0) {
            return ValidationResult.invalid("longitude out of range: " + lon);
        }

        // Timestamp freshness check
        Instant ts = event.getTimestamp();
        if (ts == null) {
            return ValidationResult.invalid("null timestamp");
        }
        long ageMs = Math.abs(Instant.now().toEpochMilli() - ts.toEpochMilli());
        if (ageMs > MAX_AGE_MS) {
            return ValidationResult.invalid("timestamp too old/stale: " + ageMs + "ms");
        }

        // Speed check (knots)
        double speed = event.getSpeed();
        if (speed < 0.0 || speed > MAX_SPEED_KNOTS) {
            return ValidationResult.invalid("speed out of range: " + speed + " kn");
        }

        return ValidationResult.valid();
    }
}
