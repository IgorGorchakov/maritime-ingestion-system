package com.maritime.common.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EnrichedVesselEvent implements Serializable {
    private VesselEvent vesselEvent;
    private boolean inRestrictedZone;
    private String zoneName;
    private double distanceToPort;
    private double riskScore;
    private String riskLevel; // LOW, MEDIUM, HIGH
}