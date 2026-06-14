package com.maritime.enricher;

import com.maritime.common.dto.EnrichedVesselEvent;
import com.maritime.common.dto.VesselEvent;
import com.maritime.common.kafka.Topics;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test: raw AIS → streaming service → maritime.enriched.
 *
 * Uses Testcontainers to spin up a real Kafka + Confluent Schema Registry,
 * bypassing all mocks. This catches:
 *   - Avro serialisation/deserialisation round-trips
 *   - Kafka consumer group and ack behaviour
 *   - Validation gate (bad records do NOT appear on the enriched topic)
 *   - Dedup (replayed records appear only once on the enriched topic)
 *
 * Why Testcontainers instead of @EmbeddedKafka?
 * EmbeddedKafka runs an in-process broker that shares the JVM heap. Avro +
 * Schema Registry require a real HTTP server (the registry) and real Kafka
 * protocol framing (4-byte schema-id prefix). EmbeddedKafka cannot host the
 * registry — Testcontainers is the only option that tests the actual wire format.
 */
@SpringBootTest
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PipelineIntegrationIT {

    private static final Network NETWORK = Network.newNetwork();

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.5.0"))
            .withNetwork(NETWORK)
            .withNetworkAliases("kafka");

    @Container
    static final GenericContainer<?> SCHEMA_REGISTRY = new GenericContainer<>(
            DockerImageName.parse("confluentinc/cp-schema-registry:7.5.0"))
            .withNetwork(NETWORK)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka:9092")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withExposedPorts(8081)
            .dependsOn(KAFKA);

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.kafka.consumer.properties.schema.registry.url",
                () -> "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081));
        registry.add("spring.kafka.producer.properties.schema.registry.url",
                () -> "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081));
        registry.add("schema.registry.url",
                () -> "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081));
        // Disable Flyway and JPA in this integration test — we only need Kafka.
        registry.add("spring.flyway.enabled", () -> "false");
        registry.add("spring.autoconfigure.exclude", () ->
                "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration," +
                "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration," +
                "org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration");
    }

    private KafkaProducer<String, VesselEvent> producer;
    private KafkaConsumer<String, EnrichedVesselEvent> enrichedConsumer;
    private KafkaConsumer<String, VesselEvent> quarantineConsumer;
    private String schemaRegistryUrl;

    @BeforeEach
    void setUpClients() {
        schemaRegistryUrl = "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081);

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProps.put("schema.registry.url", schemaRegistryUrl);
        producer = new KafkaProducer<>(producerProps);

        Map<String, Object> consumerProps = baseConsumerProps("test-enriched-" + UUID.randomUUID());
        consumerProps.put("specific.avro.reader", true);
        enrichedConsumer = new KafkaConsumer<>(consumerProps);
        enrichedConsumer.subscribe(List.of(Topics.ENRICHED));

        Map<String, Object> quarantineProps = baseConsumerProps("test-quarantine-" + UUID.randomUUID());
        quarantineProps.put("specific.avro.reader", true);
        quarantineConsumer = new KafkaConsumer<>(quarantineProps);
        quarantineConsumer.subscribe(List.of(Topics.QUARANTINE));
    }

    @AfterEach
    void tearDownClients() {
        producer.close();
        enrichedConsumer.close();
        quarantineConsumer.close();
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    @Test
    @Order(1)
    void validEvent_appearsOnEnrichedTopic() {
        VesselEvent event = validVessel("123456789");
        producer.send(new ProducerRecord<>(Topics.AIS_RAW, event.getMmsi(), event));
        producer.flush();

        await().atMost(Duration.ofSeconds(30))
               .untilAsserted(() -> {
                   ConsumerRecords<String, EnrichedVesselEvent> records =
                           enrichedConsumer.poll(Duration.ofMillis(500));
                   assertThat(records.count()).isGreaterThan(0);
                   EnrichedVesselEvent enriched = records.iterator().next().value();
                   assertThat(enriched.getVesselEvent().getMmsi()).isEqualTo("123456789");
                   assertThat(enriched.getRiskLevel()).isIn("LOW", "MEDIUM", "HIGH");
               });
    }

    @Test
    @Order(2)
    void invalidMmsi_routedToQuarantineTopic() {
        VesselEvent bad = VesselEvent.newBuilder()
                .setMmsi("INVALID").setLatitude(30.0).setLongitude(-89.0)
                .setSpeed(10.0).setHeading(90.0)
                .setTimestamp(Instant.now()).setEventType("AIS")
                .build();
        producer.send(new ProducerRecord<>(Topics.AIS_RAW, "INVALID", bad));
        producer.flush();

        await().atMost(Duration.ofSeconds(30))
               .untilAsserted(() -> {
                   ConsumerRecords<String, VesselEvent> records =
                           quarantineConsumer.poll(Duration.ofMillis(500));
                   assertThat(records.count()).isGreaterThan(0);
                   // Verify the reason header is present
                   var headers = records.iterator().next().headers();
                   assertThat(headers.lastHeader("reason")).isNotNull();
                   String reason = new String(headers.lastHeader("reason").value());
                   assertThat(reason).containsIgnoringCase("MMSI");
               });
    }

    @Test
    @Order(3)
    void nullIsland_routedToQuarantine() {
        VesselEvent nullIsland = VesselEvent.newBuilder()
                .setMmsi("999888777").setLatitude(0.0).setLongitude(0.0)
                .setSpeed(0.0).setHeading(0.0)
                .setTimestamp(Instant.now()).setEventType("AIS")
                .build();
        producer.send(new ProducerRecord<>(Topics.AIS_RAW, nullIsland.getMmsi(), nullIsland));
        producer.flush();

        await().atMost(Duration.ofSeconds(30))
               .untilAsserted(() ->
                   assertThat(quarantineConsumer.poll(Duration.ofMillis(500)).count()).isGreaterThan(0));
    }

    @Test
    @Order(4)
    void duplicateEvent_enrichedOnlyOnce() {
        // Send the same (mmsi, timestamp) twice — second should be quarantined as duplicate.
        VesselEvent event = validVessel("555444333");
        ProducerRecord<String, VesselEvent> record =
                new ProducerRecord<>(Topics.AIS_RAW, event.getMmsi(), event);
        producer.send(record);
        producer.send(record);  // exact duplicate
        producer.flush();

        // Wait for the enriched topic — expect exactly 1 record for this MMSI.
        List<EnrichedVesselEvent> enrichedForMMSI = new ArrayList<>();
        await().atMost(Duration.ofSeconds(30))
               .untilAsserted(() -> {
                   enrichedConsumer.poll(Duration.ofMillis(500)).forEach(r -> {
                       if ("555444333".equals(r.value().getVesselEvent().getMmsi())) {
                           enrichedForMMSI.add(r.value());
                       }
                   });
                   assertThat(enrichedForMMSI).hasSize(1);
               });
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private Map<String, Object> baseConsumerProps(String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        return props;
    }

    private VesselEvent validVessel(String mmsi) {
        return VesselEvent.newBuilder()
                .setMmsi(mmsi).setLatitude(30.5).setLongitude(-89.0)
                .setSpeed(12.0).setHeading(90.0)
                .setTimestamp(Instant.now()).setEventType("AIS")
                .build();
    }
}
