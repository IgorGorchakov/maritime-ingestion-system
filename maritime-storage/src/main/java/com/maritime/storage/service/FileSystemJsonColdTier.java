package com.maritime.storage.service;

import com.maritime.common.dto.EnrichedVesselEvent;
import com.maritime.common.serde.AvroJson;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * Filesystem-backed cold tier — writes one JSON file per event under a
 * Hive-style partition layout:
 * <pre>{@code <baseDir>/vessel-events/date=<yyyy-MM-dd>/mmsi=<mmsi>/<epochMs>.json}</pre>
 *
 * <p>Spark reads the same partition layout with {@code spark.read().format("json")},
 * inferring {@code date} and {@code mmsi} as virtual partition columns so no
 * metastore is required. JSON is slightly larger than Parquet but eliminates the
 * Hadoop/AWS SDK dependency entirely, which is the right trade-off for a local
 * dev stack.
 */
@Slf4j
@Service
public class FileSystemJsonColdTier implements ColdTierWriter {

    private final java.nio.file.Path baseDir;

    public FileSystemJsonColdTier(
            @Value("${maritime.cold-tier.base-dir:./data/cold}") String baseDir) {
        this.baseDir = java.nio.file.Path.of(baseDir).toAbsolutePath().normalize();
    }

    @Override
    public void write(EnrichedVesselEvent event) {
        String mmsi    = event.getVesselEvent().getMmsi();
        long   epochMs = event.getVesselEvent().getTimestamp().toEpochMilli();
        String isoDate = LocalDate.ofInstant(event.getVesselEvent().getTimestamp(), ZoneOffset.UTC).toString();

        java.nio.file.Path partitionDir = baseDir.resolve(
                String.format("vessel-events/date=%s/mmsi=%s", isoDate, mmsi));
        java.nio.file.Path target = partitionDir.resolve(epochMs + ".json");

        try {
            Files.createDirectories(partitionDir);
            Files.writeString(target, AvroJson.toJson(event), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to write cold-tier JSON for MMSI " + mmsi + " to " + target, e);
        }
        log.debug("Wrote cold-tier JSON: {}", target);
    }
}
