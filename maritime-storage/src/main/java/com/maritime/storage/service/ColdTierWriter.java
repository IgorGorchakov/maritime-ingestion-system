package com.maritime.storage.service;

import com.maritime.common.dto.EnrichedVesselEvent;

/**
 * Cold tier: append-only, partitioned event archive read by the Spark batch jobs
 * ({@code maritime-spark}).
 *
 * <p>This is a storage port — the controller depends only on this interface. The
 * default implementation ({@link FileSystemParquetColdTier}) writes Parquet to the
 * local filesystem in the same Hive-style {@code date=/mmsi=} layout Spark expects,
 * so no S3/LocalStack is required.
 */
public interface ColdTierWriter {

    /** Persist one enriched event to the partitioned cold-tier archive. */
    void write(EnrichedVesselEvent event);
}
