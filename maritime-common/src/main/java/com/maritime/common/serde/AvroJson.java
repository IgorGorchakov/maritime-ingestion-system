package com.maritime.common.serde;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/**
 * JSON (de)serialization for Avro {@link SpecificRecordBase} records.
 *
 * <p>On the Kafka wire we use compact binary Avro framed with a Schema Registry id.
 * But the non-Kafka paths — the S3 cold-tier payload and the storage-to-gateway HTTP
 * hop — are not Kafka, so they cannot reuse {@code KafkaAvroSerializer}. Plain Jackson
 * cannot round-trip a {@code SpecificRecord} (logical types, nullable unions, the
 * generated builder), so we use Avro's own JSON encoder here instead. The schema embedded
 * in the generated class stays the single source of truth across every transport.
 */
public final class AvroJson {

    private AvroJson() {
    }

    /** Serialize an Avro record to its canonical Avro-JSON representation. */
    public static <T extends SpecificRecordBase> String toJson(T record) {
        DatumWriter<T> writer = new SpecificDatumWriter<>(record.getSchema());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            Encoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), out);
            writer.write(record, encoder);
            encoder.flush();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to encode " + record.getClass().getSimpleName() + " to Avro JSON", e);
        }
        return out.toString(StandardCharsets.UTF_8);
    }

    /** Parse Avro-JSON back into the generated record type. */
    public static <T extends SpecificRecordBase> T fromJson(String json, Class<T> type) {
        T prototype = newInstance(type);
        DatumReader<T> reader = new SpecificDatumReader<>(prototype.getSchema());
        try {
            Decoder decoder = DecoderFactory.get().jsonDecoder(prototype.getSchema(), json);
            return reader.read(null, decoder);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to decode Avro JSON into " + type.getSimpleName(), e);
        }
    }

    private static <T extends SpecificRecordBase> T newInstance(Class<T> type) {
        try {
            return type.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Cannot instantiate Avro record " + type.getName(), e);
        }
    }
}
