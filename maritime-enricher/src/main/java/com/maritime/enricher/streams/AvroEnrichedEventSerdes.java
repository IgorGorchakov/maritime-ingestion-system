package com.maritime.enricher.streams;

import com.maritime.common.dto.EnrichedVesselEvent;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Avro Serde for {@link EnrichedVesselEvent}, wiring the Confluent Schema Registry
 * serializer/deserializer into the Kafka Streams DSL.
 *
 * <h3>Lifecycle</h3>
 * Each instance owns exactly one {@link KafkaAvroSerializer} and one
 * {@link KafkaAvroDeserializer}. Both wrap a {@code SchemaRegistryClient} that
 * maintains an HTTP connection pool to the registry. Constructing them on every
 * call to {@link #serializer()} / {@link #deserializer()} (the previous approach)
 * leaks that pool because the lambdas returned to Kafka Streams are never closed.
 *
 * <p>The fix: construct once in the constructor, expose the same instances on
 * every call, and implement {@link #close()} so Kafka Streams can release the
 * pool when the topology shuts down. {@link MaritimeTopology} holds this Serde
 * as a field and delegates to it from {@code @PreDestroy}.
 *
 * <h3>Kafka Streams Serde contract</h3>
 * Streams calls {@link #serializer()} and {@link #deserializer()} once per task
 * during topology initialisation. Returning the same (already-configured) instance
 * each time is both correct and efficient — Confluent's implementations are
 * thread-safe after {@code configure()}.
 */
public class AvroEnrichedEventSerdes implements Serde<EnrichedVesselEvent> {

    private final KafkaAvroSerializer   ser;
    private final KafkaAvroDeserializer deser;

    // Thin adapters that delegate to the single shared ser/deser instances.
    // Defined as fields (not anonymous lambdas) so close() reaches them.
    private final Serializer<EnrichedVesselEvent>   serializer;
    private final Deserializer<EnrichedVesselEvent> deserializer;

    public AvroEnrichedEventSerdes(String schemaRegistryUrl) {
        Map<String, Object> config = Map.of(
                "schema.registry.url",                              schemaRegistryUrl,
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true
        );

        ser  = new KafkaAvroSerializer();
        ser.configure(config, false);   // isKey = false (value serde)

        deser = new KafkaAvroDeserializer();
        deser.configure(config, false);

        serializer   = (topic, data) -> ser.serialize(topic, data);
        deserializer = (topic, data) -> (EnrichedVesselEvent) deser.deserialize(topic, data);
    }

    @Override
    public Serializer<EnrichedVesselEvent> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<EnrichedVesselEvent> deserializer() {
        return deserializer;
    }

    /**
     * Release the Schema Registry HTTP connection pool held by both the serializer
     * and deserializer. Called by {@link MaritimeTopology#stop()} via
     * {@code @PreDestroy} so the pool is drained before the JVM exits.
     */
    @Override
    public void close() {
        ser.close();
        deser.close();
    }
}
