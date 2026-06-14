package com.maritime.storage.service;

import com.maritime.common.dto.EnrichedVesselEvent;

import java.util.Optional;

/**
 * Hot tier: the latest known state per vessel, keyed by MMSI, for real-time
 * point queries ({@code GET /api/v1/vessels/{mmsi}}).
 *
 * <p>This is a storage port — the controller depends only on this interface, not
 * on whatever backs it. The default implementation
 * ({@link PostgresVesselStateHotStore}) upserts into a local Postgres table so the
 * platform runs entirely on localhost with no AWS/DynamoDB dependency.
 */
public interface VesselStateStore {

    /** Insert or replace the latest state for the event's vessel (upsert by MMSI). */
    void upsert(EnrichedVesselEvent event);

    /** Return the latest stored state for {@code mmsi}, or empty if none exists. */
    Optional<EnrichedVesselEvent> findByMmsi(String mmsi);
}
