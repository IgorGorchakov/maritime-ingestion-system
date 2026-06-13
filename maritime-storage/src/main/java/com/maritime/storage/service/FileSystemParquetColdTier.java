package com.maritime.storage.service;

import com.maritime.common.dto.EnrichedVesselEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.time.LocalDate;

/**
 * Filesystem-backed cold tier (replaces the previous S3/LocalStack implementation).
 *
 * <p>Writes one Parquet file per event under a Hive-style partition layout:
 * <pre>{@code <baseDir>/vessel-events/date=<yyyy-MM-dd>/mmsi=<mmsi>/<epochMs>.parquet}</pre>
 * The {@code date=} / {@code mmsi=} segments are read by Spark's partition discovery
 * as virtual DataFrame columns, enabling partition pruning with no metastore — the
 * same layout the S3 version produced, so {@code maritime-spark} reads it via a
 * {@code file://} path with zero query changes.
 *
 * <h3>Why Parquet (unchanged from Phase 7)</h3>
 * Columnar reads (Spark scans ~6 of ~15 columns), partition pruning on {@code date},
 * and Snappy compression (3–6× vs JSON for floating-point AIS data).
 *
 * <h3>Why no temp file anymore</h3>
 * The S3 version wrote Parquet to a local temp file, then uploaded the bytes via
 * {@code putObject}, then deleted the temp. With a local cold tier the temp file
 * <em>is</em> the destination — we write {@code AvroParquetWriter} straight to the
 * final partition path (Hadoop's {@code LocalFileSystem}), so the upload and cleanup
 * steps simply disappear.
 */
@Slf4j
@Service
public class FileSystemParquetColdTier implements ColdTierWriter {

    private final java.nio.file.Path baseDir;

    public FileSystemParquetColdTier(
            @Value("${maritime.cold-tier.base-dir:./data/cold}") String baseDir) {
        this.baseDir = java.nio.file.Path.of(baseDir).toAbsolutePath().normalize();
    }

    @Override
    public void write(EnrichedVesselEvent event) {
        String mmsi    = event.getVesselEvent().getMmsi();
        long   epochMs = event.getVesselEvent().getTimestamp().toEpochMilli();
        String isoDate = LocalDate.now().toString();   // partition by ingest date

        java.nio.file.Path partitionDir = baseDir.resolve(
                String.format("vessel-events/date=%s/mmsi=%s", isoDate, mmsi));
        java.nio.file.Path target = partitionDir.resolve(epochMs + ".parquet");

        try {
            Files.createDirectories(partitionDir);
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to create cold-tier partition dir " + partitionDir, e);
        }

        Schema schema = event.getSchema();
        Path hadoopPath = new Path(target.toUri());
        Configuration hadoopConf = new Configuration();

        try (ParquetWriter<EnrichedVesselEvent> writer =
                     AvroParquetWriter.<EnrichedVesselEvent>builder(hadoopPath)
                             .withSchema(schema)
                             .withCompressionCodec(CompressionCodecName.SNAPPY)
                             .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                             .withConf(hadoopConf)
                             .build()) {

            writer.write(event);

        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to write Parquet for MMSI " + mmsi + " to " + target, e);
        }
        log.debug("Wrote cold-tier Parquet: {}", target);
    }
}
