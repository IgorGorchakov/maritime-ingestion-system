package com.maritime.detection.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * JSON-based Serde for {@link VesselState}, used by the Kafka Streams RocksDB
 * state store and its internal changelog topic.
 *
 * JSON (not Avro) is used intentionally: {@link VesselState} is internal topology
 * state that never leaves the detection service. Adding it to Schema Registry would
 * create schema-governance overhead with no consumer benefit. JSON is also human-
 * readable in the changelog topic, which aids debugging.
 */
public class VesselStateSerdes implements Serde<VesselState> {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final Serializer<VesselState>   serializer;
    private final Deserializer<VesselState> deserializer;

    public VesselStateSerdes() {
        serializer = (topic, data) -> {
            if (data == null) return null;
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize VesselState", e);
            }
        };
        deserializer = (topic, data) -> {
            if (data == null) return null;
            try {
                return MAPPER.readValue(data, VesselState.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize VesselState", e);
            }
        };
    }

    @Override public Serializer<VesselState>   serializer()   { return serializer; }
    @Override public Deserializer<VesselState> deserializer() { return deserializer; }
}
