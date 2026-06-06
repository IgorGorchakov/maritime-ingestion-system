package com.maritime.storage.service;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

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
        this.tableName = tableName;

        BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKey, secretKey);
        ClientConfiguration clientConfig = new ClientConfiguration()
                .withSignerOverride("AWSS3V4SignerType");

        this.s3Client = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new EndpointConfiguration(endpoint, Regions.US_EAST_1.getName()))
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .withClientConfiguration(clientConfig)
                .withPathStyleAccessEnabled(true)
                .build();

        this.dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new EndpointConfiguration(endpoint, Regions.US_EAST_1.getName()))
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .build();
    }

    /**
     * Ensure the S3 bucket and DynamoDB table exist before any read/write.
     * LocalStack starts empty, so we create them on startup.
     */
    @PostConstruct
    public void initResources() {
        if (!s3Client.doesBucketExistV2(bucketName)) {
            s3Client.createBucket(bucketName);
            System.out.println("Created S3 bucket: " + bucketName);
        }

        try {
            dynamoDBClient.createTable(new CreateTableRequest()
                    .withTableName(tableName)
                    .withKeySchema(new KeySchemaElement("mmsi", KeyType.HASH))
                    .withAttributeDefinitions(new AttributeDefinition("mmsi", ScalarAttributeType.S))
                    .withProvisionedThroughput(new ProvisionedThroughput(5L, 5L)));
            System.out.println("Created DynamoDB table: " + tableName);
        } catch (ResourceInUseException e) {
            // Table already exists, nothing to do.
        }
    }

    public void saveToS3(String key, String content) {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(bytes.length);
        metadata.setContentType("application/json");
        s3Client.putObject(bucketName, key, new ByteArrayInputStream(bytes), metadata);
        System.out.println("Saved to S3: " + key);
    }

    public void saveToDynamoDB(Map<String, AttributeValue> item) {
        dynamoDBClient.putItem(tableName, item);
        System.out.println("Saved to DynamoDB: " + tableName);
    }

    /**
     * Look up the latest risk record for a vessel from DynamoDB.
     * Returns null if no record exists yet.
     */
    public Map<String, AttributeValue> getVesselRisk(String mmsi) {
        GetItemResult result = dynamoDBClient.getItem(new GetItemRequest()
                .withTableName(tableName)
                .withKey(Map.of("mmsi", new AttributeValue().withS(mmsi))));
        return result.getItem();
    }
}
