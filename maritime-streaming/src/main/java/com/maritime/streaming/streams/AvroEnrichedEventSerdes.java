package com.maritime.streaming.streams;

import com.maritime.common.dto.EnrichedVesselEvent;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Avro Serde for {@link EnrichedVesselEvent}, wiring the Confluent Schema Registry
 * serializer/deserializer into the Kafka Streams DSL.
 *
 * Streams requires a Serde rather than the plain Serializer/Deserializer used by
 * spring-kafka @KafkaListener containers — this adapter bridges the two.
 */
public class AvroEnrichedEventSerdes implements Serde<EnrichedVesselEvent> {

    private final String schemaRegistryUrl;

    public AvroEnrichedEventSerdes(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public Serializer<EnrichedVesselEvent> serializer() {
        KafkaAvroSerializer ser = new KafkaAvroSerializer();
        Map<String, Object> config = new HashMap<>();
        config.put("schema.registry.url", schemaRegistryUrl);
        ser.configure(config, false);
        return (topic, data) -> ser.serialize(topic, data);
    }

    @Override
    public Deserializer<EnrichedVesselEvent> deserializer() {
        KafkaAvroDeserializer deser = new KafkaAvroDeserializer();
        Map<String, Object> config = new HashMap<>();
        config.put("schema.registry.url", schemaRegistryUrl);
        config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        deser.configure(config, false);
        return (topic, data) -> (EnrichedVesselEvent) deser.deserialize(topic, data);
    }
}
