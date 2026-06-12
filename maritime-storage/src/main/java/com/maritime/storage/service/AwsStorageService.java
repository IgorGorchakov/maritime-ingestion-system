package com.maritime.storage.service;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.maritime.common.dto.EnrichedVesselEvent;
import jakarta.annotation.PostConstruct;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.Map;

/**
 * Writes vessel events to the S3 cold tier as Parquet (Phase 7) and keeps
 * DynamoDB as the hot tier for real-time point queries.
 *
 * <h3>Why Parquet and not JSON?</h3>
 * <ul>
 *   <li><b>Columnar reads:</b> Spark's DailyVesselAggregatesJob reads only
 *       {@code mmsi}, {@code speed}, {@code riskScore}, and the detection flags —
 *       roughly 6 of the ~15 schema columns. Parquet skips the other 9 entirely,
 *       which reduces I/O by ~60%.</li>
 *   <li><b>Partition pruning:</b> storing one file per {@code date=<iso>/mmsi=<id>}
 *       prefix lets Spark push a {@code WHERE date = '2024-01-15'} predicate to S3
 *       list calls, so a daily job reads exactly one day's files rather than the
 *       full bucket.</li>
 *   <li><b>Snappy compression:</b> typical 3-6× size reduction vs JSON for
 *       floating-point AIS data, with negligible decompression overhead.</li>
 * </ul>
 *
 * <h3>Write strategy</h3>
 * Parquet requires a seekable output stream, which the AWS SDK's PutObject does
 * not provide directly. We therefore:
 * <ol>
 *   <li>Write the Parquet file to a local temp file via {@code AvroParquetWriter}
 *       (backed by Hadoop's LocalFileSystem — no HDFS/S3A needed).</li>
 *   <li>Upload the temp file to S3 as a single {@code putObject} call.</li>
 *   <li>Delete the temp file.</li>
 * </ol>
 * This avoids pulling in {@code hadoop-aws} / S3A into the Spring Boot service
 * classpath — S3A would conflict with Spring Boot's managed AWS SDK versions.
 */
@Slf4j
@Service
public class AwsStorageService {

    private final AmazonS3 s3Client;
    private final AmazonDynamoDB dynamoDBClient;
    private final String bucketName;
    private final String tableName;

    public AwsStorageService(
            @Value("${aws.endpoint.localstack:http://localhost:4566}") String endpoint,
            @Value("${aws.access-key:test}") String accessKey,
            @Value("${aws.secret-key:test}") String secretKey,
            @Value("${aws.s3.bucket-name:maritime-data}") String bucketName,
            @Value("${aws.dynamodb.table-name:vessel-risk}") String tableName) {

        this.bucketName = bucketName;
        this.tableName  = tableName;

        BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKey, secretKey);
        ClientConfiguration clientConfig = new ClientConfiguration()
                .withSignerOverride("AWSS3V4SignerType");

        this.s3Client = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(
                        new EndpointConfiguration(endpoint, Regions.US_EAST_1.getName()))
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .withClientConfiguration(clientConfig)
                .withPathStyleAccessEnabled(true)
                .build();

        this.dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(
                        new EndpointConfiguration(endpoint, Regions.US_EAST_1.getName()))
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .build();
    }

    @PostConstruct
    public void initResources() {
        if (!s3Client.doesBucketExistV2(bucketName)) {
            s3Client.createBucket(bucketName);
            log.info("Created S3 bucket: {}", bucketName);
        }

        try {
            dynamoDBClient.createTable(new CreateTableRequest()
                    .withTableName(tableName)
                    .withKeySchema(new KeySchemaElement("mmsi", KeyType.HASH))
                    .withAttributeDefinitions(
                            new AttributeDefinition("mmsi", ScalarAttributeType.S))
                    .withProvisionedThroughput(new ProvisionedThroughput(5L, 5L)));
            log.info("Created DynamoDB table: {}", tableName);
        } catch (ResourceInUseException e) {
            // Table already exists — idempotent startup.
        }
    }

    /**
     * Write one {@link EnrichedVesselEvent} to the S3 Parquet cold tier.
     *
     * <p>S3 key layout: {@code vessel-events/date=<yyyy-MM-dd>/mmsi=<mmsi>/<epochMs>.parquet}
     * <br>The {@code date=} and {@code mmsi=} segments are Hive-style partition
     * columns that Spark's partition discovery reads as virtual DataFrame columns,
     * enabling partition pruning without any metastore.
     *
     * @param event the enriched event to persist
     */
    public void saveParquetToS3(EnrichedVesselEvent event) {
        String mmsi      = event.getVesselEvent().getMmsi();
        long   epochMs   = event.getVesselEvent().getTimestamp().toEpochMilli();
        String isoDate   = LocalDate.now().toString();   // partition by ingest date

        String s3Key = String.format(
                "vessel-events/date=%s/mmsi=%s/%d.parquet", isoDate, mmsi, epochMs);

        // ── 1. Write Parquet to a local temp file ─────────────────────────────
        java.io.File tempFile;
        try {
            tempFile = Files.createTempFile("vessel-", ".parquet").toFile();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create temp file for Parquet write", e);
        }

        Schema schema = event.getSchema();
        Path hadoopPath = new Path(tempFile.toURI());
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
            tempFile.delete();
            throw new UncheckedIOException(
                    "Failed to write Parquet for MMSI " + mmsi, e);
        }

        // ── 2. Upload to S3 ───────────────────────────────────────────────────
        try {
            byte[] parquetBytes = Files.readAllBytes(tempFile.toPath());
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(parquetBytes.length);
            meta.setContentType("application/octet-stream");
            s3Client.putObject(
                    bucketName, s3Key,
                    new ByteArrayInputStream(parquetBytes), meta);
            log.debug("Saved Parquet to S3: {}", s3Key);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read temp Parquet file for upload", e);
        } finally {
            tempFile.delete();
        }
    }

    public void saveToDynamoDB(Map<String, AttributeValue> item) {
        dynamoDBClient.putItem(tableName, item);
        log.debug("Saved to DynamoDB: {}", tableName);
    }

    public Map<String, AttributeValue> getVesselRisk(String mmsi) {
        GetItemResult result = dynamoDBClient.getItem(new GetItemRequest()
                .withTableName(tableName)
                .withKey(Map.of("mmsi", new AttributeValue().withS(mmsi))));
        return result.getItem();
    }
}
