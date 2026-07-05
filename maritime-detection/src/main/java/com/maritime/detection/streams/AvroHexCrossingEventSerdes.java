package com.maritime.detection.streams;

import com.maritime.common.dto.HexCrossingEvent;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Avro Serde for {@link HexCrossingEvent}, wiring the Confluent Schema Registry
 * serializer/deserializer into the Kafka Streams DSL for the
 * {@code maritime.hex.crossings} output topic.
 *
 * <p>Mirrors {@link AvroEnrichedEventSerdes} in structure and lifecycle.
 * {@link MaritimeTopology} holds one instance as a field and closes it in
 * {@code @PreDestroy} after {@code KafkaStreams.close()} returns.
 */
public class AvroHexCrossingEventSerdes implements Serde<HexCrossingEvent> {

    private final KafkaAvroSerializer             ser;
    private final KafkaAvroDeserializer           deser;
    private final Serializer<HexCrossingEvent>   serializer;
    private final Deserializer<HexCrossingEvent> deserializer;

    public AvroHexCrossingEventSerdes(String schemaRegistryUrl) {
        Map<String, Object> config = Map.of(
                "schema.registry.url",                                    schemaRegistryUrl,
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true
        );
        ser  = new KafkaAvroSerializer();
        ser.configure(config, false);
        deser = new KafkaAvroDeserializer();
        deser.configure(config, false);

        serializer   = ser::serialize;
        deserializer = (topic, data) -> (HexCrossingEvent) deser.deserialize(topic, data);
    }

    @Override public Serializer<HexCrossingEvent>   serializer()   { return serializer; }
    @Override public Deserializer<HexCrossingEvent> deserializer() { return deserializer; }

    /** Releases the Schema Registry HTTP connection pool. Called by {@link MaritimeTopology#stop()}. */
    @Override
    public void close() {
        ser.close();
        deser.close();
    }
}
