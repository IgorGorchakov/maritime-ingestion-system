package com.maritime.storage;

import com.maritime.common.dto.EnrichedVesselEvent;
import com.maritime.common.dto.VesselEvent;
import com.maritime.storage.service.ColdTierWriter;
import com.maritime.storage.service.VesselStateStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the storage tier.
 *
 * <h3>Scope</h3>
 * Tests the hot tier ({@link VesselStateStore} → Postgres) and cold tier
 * ({@link ColdTierWriter} → local Parquet) directly via the storage port
 * interfaces, without involving Kafka. This isolates persistence behaviour from
 * broker availability and runs in CI with no external infrastructure beyond
 * the Testcontainers Postgres instance spun up here.
 *
 * <h3>Why Testcontainers and not H2?</h3>
 * {@code vessel_risk} relies on Postgres-specific syntax:
 * <ul>
 *   <li>{@code ::jsonb} cast for the {@code payload} column</li>
 *   <li>{@code ON CONFLICT (mmsi) DO UPDATE SET …} upsert semantics</li>
 * </ul>
 * H2's Postgres-compatibility mode supports neither reliably. Running against a
 * real {@code postgis/postgis:15-3.3} image — the same one in {@code docker-compose.yml}
 * — catches type errors and constraint issues that a stub database would hide.
 *
 * <h3>Kafka exclusion</h3>
 * {@code KafkaAutoConfiguration} is excluded via {@code spring.autoconfigure.exclude}
 * so the application context starts without trying to connect to a broker. The
 * {@link com.maritime.storage.config.KafkaConsumerConfig} bean is still loaded
 * (it requires a {@code KafkaTemplate}), so we point bootstrap-servers at a
 * dummy address — the listener container never starts because Kafka auto-config
 * is excluded before it tries to connect.
 */
@SpringBootTest
@Testcontainers
class StorageIntegrationIT {

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgis/postgis:15-3.3")
                    .withDatabaseName("maritime")
                    .withUsername("postgres")
                    .withPassword("postgres");

    /** JUnit creates a fresh temp directory per test class; wiped after the suite. */
    @TempDir
    static Path coldTierBase;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry reg) {
        // ── Postgres ────────────────────────────────────────────────────────
        reg.add("spring.datasource.url",      POSTGRES::getJdbcUrl);
        reg.add("spring.datasource.username", POSTGRES::getUsername);
        reg.add("spring.datasource.password", POSTGRES::getPassword);
        // Always run schema.sql so the vessel_risk table exists in the fresh DB.
        reg.add("spring.sql.init.mode", () -> "always");

        // ── Cold tier ───────────────────────────────────────────────────────
        // Override the base directory so Parquet files land in the JUnit temp
        // dir and are cleaned up automatically after the test suite.
        reg.add("maritime.cold-tier.base-dir", coldTierBase::toString);

        // ── Kafka — disabled ────────────────────────────────────────────────
        // No broker is running in this test; exclude auto-config so the context
        // starts without attempting a broker connection.
        reg.add("spring.kafka.bootstrap-servers", () -> "localhost:19092");
        reg.add("spring.autoconfigure.exclude",   () ->
                "org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration");
    }

    @Autowired VesselStateStore stateStore;
    @Autowired ColdTierWriter   coldTier;

    // ── Fixture builder ───────────────────────────────────────────────────────

    static EnrichedVesselEvent event(String mmsi, String riskLevel, boolean loitering) {
        VesselEvent vessel = VesselEvent.newBuilder()
                .setMmsi(mmsi)
                .setLatitude(30.5).setLongitude(-89.0)
                .setSpeed(12.0).setHeading(90.0)
                .setTimestamp(Instant.now())
                .setEventType("AIS")
                .build();

        return EnrichedVesselEvent.newBuilder()
                .setVesselEvent(vessel)
                .setInRestrictedZone(false)
                .setZoneName(null).setZoneType(null)
                .setDistanceToPort(25.0)
                .setRiskScore("HIGH".equals(riskLevel) ? 60.0 : 15.0)
                .setRiskLevel(riskLevel)
                .setLoitering(loitering)
                .setDarkVessel(false)
                .setSpeedAnomaly(false)
                .build();
    }

    // ── Hot tier ──────────────────────────────────────────────────────────────

    @Test
    @DisplayName("upsert then findByMmsi round-trips all scalar fields via jsonb")
    void hotTier_upsertAndRetrieve_roundTrip() {
        stateStore.upsert(event("123456789", "LOW", false));

        Optional<EnrichedVesselEvent> found = stateStore.findByMmsi("123456789");

        assertThat(found).isPresent();
        assertThat(found.get().getVesselEvent().getMmsi()).isEqualTo("123456789");
        assertThat(found.get().getRiskLevel()).isEqualTo("LOW");
        assertThat(found.get().getLoitering()).isFalse();
    }

    @Test
    @DisplayName("second upsert overwrites — ON CONFLICT DO UPDATE keeps only the latest state")
    void hotTier_secondUpsert_overwritesFirst() {
        stateStore.upsert(event("111222333", "LOW",  false));
        stateStore.upsert(event("111222333", "HIGH", true));

        EnrichedVesselEvent found = stateStore.findByMmsi("111222333").orElseThrow();

        assertThat(found.getRiskLevel()).isEqualTo("HIGH");
        assertThat(found.getLoitering()).isTrue();
    }

    @Test
    @DisplayName("findByMmsi returns empty Optional for an unknown MMSI")
    void hotTier_findByMmsi_unknown_returnsEmpty() {
        assertThat(stateStore.findByMmsi("000000000")).isEmpty();
    }

    @Test
    @DisplayName("all detection flags and nullable zone fields survive the jsonb round-trip")
    void hotTier_detectionFlagsAndZoneFields_surviveJsonbRoundTrip() {
        VesselEvent vessel = VesselEvent.newBuilder()
                .setMmsi("444555666")
                .setLatitude(51.5).setLongitude(-0.1)
                .setSpeed(0.3).setHeading(0.0)
                .setTimestamp(Instant.now())
                .setEventType("AIS")
                .build();

        EnrichedVesselEvent event = EnrichedVesselEvent.newBuilder()
                .setVesselEvent(vessel)
                .setInRestrictedZone(true)
                .setZoneName("English Channel EEZ")
                .setZoneType("EEZ")
                .setDistanceToPort(3.5)
                .setRiskScore(70.0)
                .setRiskLevel("HIGH")
                .setLoitering(true)
                .setDarkVessel(true)
                .setSpeedAnomaly(true)
                .build();

        stateStore.upsert(event);

        EnrichedVesselEvent found = stateStore.findByMmsi("444555666").orElseThrow();
        assertThat(found.getLoitering()).isTrue();
        assertThat(found.getDarkVessel()).isTrue();
        assertThat(found.getSpeedAnomaly()).isTrue();
        assertThat(found.getInRestrictedZone()).isTrue();
        assertThat(found.getZoneName()).isEqualTo("English Channel EEZ");
        assertThat(found.getZoneType()).isEqualTo("EEZ");
        assertThat(found.getRiskScore()).isEqualTo(70.0);
    }

    // ── Cold tier ─────────────────────────────────────────────────────────────

    @Test
    @DisplayName("write creates a .parquet file under the Hive-partitioned path")
    void coldTier_write_createsParquetFileAtPartitionPath() {
        EnrichedVesselEvent event = event("777888999", "MEDIUM", false);
        String mmsi    = event.getVesselEvent().getMmsi();
        String isoDate = LocalDate.now().toString();

        coldTier.write(event);

        // Expected: <base>/vessel-events/date=<date>/mmsi=<mmsi>/<epochMs>.parquet
        Path partitionDir = coldTierBase
                .resolve("vessel-events")
                .resolve("date=" + isoDate)
                .resolve("mmsi=" + mmsi);

        assertThat(partitionDir).isDirectory();
        assertThat(listParquetFiles(partitionDir)).isGreaterThan(0);
    }

    @Test
    @DisplayName("writing two events for the same vessel produces at least one Parquet file")
    void coldTier_write_twoEventsForSameVessel_parquetFilesPresent() throws InterruptedException {
        String mmsi    = "321321321";
        String isoDate = LocalDate.now().toString();

        coldTier.write(event(mmsi, "LOW", false));
        Thread.sleep(10); // ensure distinct epoch-ms filenames
        coldTier.write(event(mmsi, "LOW", false));

        Path partitionDir = coldTierBase
                .resolve("vessel-events")
                .resolve("date=" + isoDate)
                .resolve("mmsi=" + mmsi);

        // FileSystemParquetColdTier uses ParquetFileWriter.Mode.OVERWRITE so two
        // writes with the same epoch-ms key produce one file; distinct timestamps
        // produce two. Either way at least one file must be present.
        assertThat(listParquetFiles(partitionDir)).isGreaterThanOrEqualTo(1);
    }

    @Test
    @DisplayName("different vessels land in separate mmsi= partition directories")
    void coldTier_write_differentVessels_separatePartitionDirectories() {
        coldTier.write(event("111111111", "LOW", false));
        coldTier.write(event("222222222", "LOW", false));

        String isoDate = LocalDate.now().toString();
        Path dateDir   = coldTierBase.resolve("vessel-events").resolve("date=" + isoDate);

        assertThat(dateDir.resolve("mmsi=111111111")).isDirectory();
        assertThat(dateDir.resolve("mmsi=222222222")).isDirectory();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private long listParquetFiles(Path dir) {
        try (var stream = Files.list(dir)) {
            return stream.filter(p -> p.toString().endsWith(".parquet")).count();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
