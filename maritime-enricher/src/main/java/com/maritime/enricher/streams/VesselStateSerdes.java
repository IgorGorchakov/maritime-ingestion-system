package com.maritime.enricher.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * JSON-based Serde for {@link VesselState}, used by the Kafka Streams state store.
 * <p>
 * We use JSON (not Avro) here intentionally: VesselState is internal topology
 * state that never leaves the streaming service — it lives in RocksDB and the
 * internal changelog topic. Adding it to Schema Registry would create schema-
 * governance overhead with no consumer benefit.
 */
public class VesselStateSerdes implements Serde<VesselState> {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Override
    public Serializer<VesselState> serializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize VesselState", e);
            }
        };
    }

    @Override
    public Deserializer<VesselState> deserializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                return MAPPER.readValue(data, VesselState.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize VesselState", e);
            }
        };
    }
}
