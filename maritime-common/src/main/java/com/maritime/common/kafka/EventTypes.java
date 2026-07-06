package com.maritime.common.kafka;

/**
 * Compile-time constants for the {@code eventType} field on vessel events.
 *
 * <p>The literal {@code "AIS"} was previously hardcoded independently in
 * {@code AisSimulatorService} and {@code VesselDetectionProcessor}. A single
 * constant here ensures a typo or future taxonomy change is caught at
 * compile time and applied in one place.
 */
public final class EventTypes {

    private EventTypes() {}

    /** Standard AIS position report sourced from a VHF or satellite receiver. */
    public static final String AIS = "AIS";
}
