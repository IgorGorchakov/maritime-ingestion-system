package com.maritime.common.validation;

import com.maritime.common.dto.VesselEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link VesselEventValidator}.
 *
 * Tests every guard in the validator independently, plus boundary values.
 * No Spring context, no Kafka — pure unit tests that run in milliseconds.
 */
class VesselEventValidatorTest {

    private VesselEventValidator validator;

    @BeforeEach
    void setUp() {
        validator = new VesselEventValidator();
    }

    // ── Happy path ────────────────────────────────────────────────────────────

    @Test
    void validEvent_passes() {
        VesselEvent event = vessel("123456789", 30.5, -89.0, 12.0);
        assertThat(validator.validate(event).isValid()).isTrue();
    }

    // ── MMSI validation ───────────────────────────────────────────────────────

    @ParameterizedTest(name = "MMSI ''{0}'' rejected")
    @ValueSource(strings = {"12345678", "1234567890", "12345678A", "", "ABCDEFGHI"})
    void invalidMmsi_rejected(String mmsi) {
        ValidationResult result = validator.validate(vessel(mmsi, 30.0, -89.0, 10.0));
        assertThat(result.isValid()).isFalse();
        assertThat(result.getReason()).contains("MMSI");
    }

    @Test
    void nullMmsi_rejected() {
        // The Avro builder rejects a null mmsi up front, so build a valid event and
        // null the field via the plain setter (which does not validate) to exercise
        // the validator's own null guard.
        VesselEvent event = vessel("123456789", 30.0, -89.0, 10.0);
        event.setMmsi(null);
        ValidationResult result = validator.validate(event);
        assertThat(result.isValid()).isFalse();
    }

    // ── Null island ───────────────────────────────────────────────────────────

    @Test
    void nullIsland_rejected() {
        ValidationResult result = validator.validate(vessel("123456789", 0.0, 0.0, 0.0));
        assertThat(result.isValid()).isFalse();
        assertThat(result.getReason()).contains("null island");
    }

    // ── Latitude bounds ───────────────────────────────────────────────────────

    @ParameterizedTest(name = "lat={0} out of range")
    @CsvSource({"91.0,-89.0", "-91.0,-89.0"})
    void outOfRangeLatitude_rejected(double lat, double lon) {
        ValidationResult result = validator.validate(vessel("123456789", lat, lon, 5.0));
        assertThat(result.isValid()).isFalse();
        assertThat(result.getReason()).contains("latitude");
    }

    @ParameterizedTest(name = "lat={0} boundary accepted")
    @CsvSource({"90.0,-89.0", "-90.0,-89.0"})
    void boundaryLatitude_accepted(double lat, double lon) {
        assertThat(validator.validate(vessel("123456789", lat, lon, 5.0)).isValid()).isTrue();
    }

    // ── Longitude bounds ──────────────────────────────────────────────────────

    @ParameterizedTest(name = "lon={1} out of range")
    @CsvSource({"30.0,181.0", "30.0,-181.0"})
    void outOfRangeLongitude_rejected(double lat, double lon) {
        ValidationResult result = validator.validate(vessel("123456789", lat, lon, 5.0));
        assertThat(result.isValid()).isFalse();
        assertThat(result.getReason()).contains("longitude");
    }

    // ── Speed bounds ──────────────────────────────────────────────────────────

    @Test
    void negativeSpeed_rejected() {
        ValidationResult result = validator.validate(vessel("123456789", 30.0, -89.0, -1.0));
        assertThat(result.isValid()).isFalse();
        assertThat(result.getReason()).contains("speed");
    }

    @Test
    void excessiveSpeed_rejected() {
        ValidationResult result = validator.validate(vessel("123456789", 30.0, -89.0, 103.0));
        assertThat(result.isValid()).isFalse();
        assertThat(result.getReason()).contains("speed");
    }

    @Test
    void maxLegalSpeed_accepted() {
        // 102.2 kn is the configured ceiling
        assertThat(validator.validate(vessel("123456789", 30.0, -89.0, 102.2)).isValid()).isTrue();
    }

    // ── Timestamp staleness ───────────────────────────────────────────────────

    @Test
    void staleTimestamp_rejected() {
        VesselEvent event = VesselEvent.newBuilder()
                .setMmsi("123456789").setLatitude(30.0).setLongitude(-89.0)
                .setSpeed(10.0).setHeading(90.0)
                .setTimestamp(Instant.now().minusSeconds(90_000))  // 25 hours ago
                .setEventType("AIS")
                .build();
        ValidationResult result = validator.validate(event);
        assertThat(result.isValid()).isFalse();
        assertThat(result.getReason()).contains("timestamp");
    }

    @Test
    void freshTimestamp_accepted() {
        VesselEvent event = VesselEvent.newBuilder()
                .setMmsi("123456789").setLatitude(30.0).setLongitude(-89.0)
                .setSpeed(10.0).setHeading(90.0)
                .setTimestamp(Instant.now())
                .setEventType("AIS")
                .build();
        assertThat(validator.validate(event).isValid()).isTrue();
    }

    // ── Helper ────────────────────────────────────────────────────────────────

    private VesselEvent vessel(String mmsi, double lat, double lon, double speed) {
        return VesselEvent.newBuilder()
                .setMmsi(mmsi).setLatitude(lat).setLongitude(lon)
                .setSpeed(speed).setHeading(90.0)
                .setTimestamp(Instant.now()).setEventType("AIS")
                .build();
    }
}
