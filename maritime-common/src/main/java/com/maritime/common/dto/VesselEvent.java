package com.maritime.common.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class VesselEvent implements Serializable {
    private String mmsi;
    private double latitude;
    private double longitude;
    private double speed;
    private double heading;
    private Instant timestamp;
    private String eventType; // AIS, SATELLITE, etc.
}