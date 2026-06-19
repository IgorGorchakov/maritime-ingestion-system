package com.maritime.detection.streams;

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
 * {@link KafkaAvroDeserializer}, both constructed and configured in the constructor.
 * Both wrap a {@code SchemaRegistryClient} that maintains an HTTP connection pool.
 * Constructing them on every call to {@link #serializer()} / {@link #deserializer()}
 * would leak that pool because the lambdas returned to Kafka Streams are never closed.
 *
 * <p>{@link MaritimeTopology} holds two instances ({@code consumeSerde} and
 * {@code produceSerde}) as final fields and calls {@link #close()} in its
 * {@code @PreDestroy} method after {@code KafkaStreams.close()} has returned,
 * ensuring no in-flight serialization is interrupted.
 */
public class AvroEnrichedEventSerdes implements Serde<EnrichedVesselEvent> {

    private final KafkaAvroSerializer               ser;
    private final KafkaAvroDeserializer             deser;
    private final Serializer<EnrichedVesselEvent>   serializer;
    private final Deserializer<EnrichedVesselEvent> deserializer;

    public AvroEnrichedEventSerdes(String schemaRegistryUrl) {
        Map<String, Object> config = Map.of(
                "schema.registry.url",                                    schemaRegistryUrl,
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true
        );
        ser  = new KafkaAvroSerializer();
        ser.configure(config, false);
        deser = new KafkaAvroDeserializer();
        deser.configure(config, false);

        serializer   = ser::serialize;
        deserializer = (topic, data) -> (EnrichedVesselEvent) deser.deserialize(topic, data);
    }

    @Override public Serializer<EnrichedVesselEvent>   serializer()   { return serializer; }
    @Override public Deserializer<EnrichedVesselEvent> deserializer() { return deserializer; }

    /** Releases the Schema Registry HTTP connection pool. Called by {@link MaritimeTopology#stop()}. */
    @Override
    public void close() {
        ser.close();
        deser.close();
    }
}
