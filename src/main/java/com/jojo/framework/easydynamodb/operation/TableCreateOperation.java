package com.jojo.framework.easydynamodb.operation;

import com.jojo.framework.easydynamodb.exception.DynamoException;
import com.jojo.framework.easydynamodb.logging.DdmLogger;
import com.jojo.framework.easydynamodb.metadata.EntityMetadata;
import com.jojo.framework.easydynamodb.metadata.FieldMetadata;
import com.jojo.framework.easydynamodb.metadata.GsiMetadata;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Creates a DynamoDB table based on entity metadata. Infers key schema and
 * attribute types from the entity's partition key and optional sort key,
 * uses PAY_PER_REQUEST billing, and waits for the table to become ACTIVE.
 */
public class TableCreateOperation {

    private static final DdmLogger log = DdmLogger.getLogger(TableCreateOperation.class);

    private final DynamoDbClient dynamoDbClient;

    public TableCreateOperation(DynamoDbClient dynamoDbClient) {
        this.dynamoDbClient = dynamoDbClient;
    }

    /**
     * Creates a DynamoDB table derived from the given entity metadata.
     *
     * @param metadata the entity metadata describing table name and keys
     * @throws DynamoException if table creation or waiting fails
     */
    public void createTable(EntityMetadata metadata) {
        String tableName = metadata.getTableName();
        log.info("Creating table: {}", tableName);
        FieldMetadata pk = metadata.getPartitionKey();
        FieldMetadata sk = metadata.getSortKey();

        List<KeySchemaElement> keySchema = new ArrayList<>();
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        Set<String> definedAttributes = new HashSet<>();

        // Partition key (HASH)
        keySchema.add(KeySchemaElement.builder()
                .attributeName(pk.getDynamoAttributeName())
                .keyType(KeyType.HASH)
                .build());
        attributeDefinitions.add(AttributeDefinition.builder()
                .attributeName(pk.getDynamoAttributeName())
                .attributeType(toDynamoScalarType(pk.getFieldType()))
                .build());
        definedAttributes.add(pk.getDynamoAttributeName());

        // Sort key (RANGE) — optional
        if (sk != null) {
            keySchema.add(KeySchemaElement.builder()
                    .attributeName(sk.getDynamoAttributeName())
                    .keyType(KeyType.RANGE)
                    .build());
            attributeDefinitions.add(AttributeDefinition.builder()
                    .attributeName(sk.getDynamoAttributeName())
                    .attributeType(toDynamoScalarType(sk.getFieldType()))
                    .build());
            definedAttributes.add(sk.getDynamoAttributeName());
        }

        // GSI definitions
        List<GlobalSecondaryIndex> gsiList = new ArrayList<>();
        for (GsiMetadata gsi : metadata.getGlobalSecondaryIndexes()) {
            List<KeySchemaElement> gsiKeySchema = new ArrayList<>();
            FieldMetadata gsiPk = gsi.getPartitionKey();
            gsiKeySchema.add(KeySchemaElement.builder()
                    .attributeName(gsiPk.getDynamoAttributeName())
                    .keyType(KeyType.HASH)
                    .build());
            if (!definedAttributes.contains(gsiPk.getDynamoAttributeName())) {
                attributeDefinitions.add(AttributeDefinition.builder()
                        .attributeName(gsiPk.getDynamoAttributeName())
                        .attributeType(toDynamoScalarType(gsiPk.getFieldType()))
                        .build());
                definedAttributes.add(gsiPk.getDynamoAttributeName());
            }

            FieldMetadata gsiSk = gsi.getSortKey();
            if (gsiSk != null) {
                gsiKeySchema.add(KeySchemaElement.builder()
                        .attributeName(gsiSk.getDynamoAttributeName())
                        .keyType(KeyType.RANGE)
                        .build());
                if (!definedAttributes.contains(gsiSk.getDynamoAttributeName())) {
                    attributeDefinitions.add(AttributeDefinition.builder()
                            .attributeName(gsiSk.getDynamoAttributeName())
                            .attributeType(toDynamoScalarType(gsiSk.getFieldType()))
                            .build());
                    definedAttributes.add(gsiSk.getDynamoAttributeName());
                }
            }

            gsiList.add(GlobalSecondaryIndex.builder()
                    .indexName(gsi.getIndexName())
                    .keySchema(gsiKeySchema)
                    .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                    .build());
        }

        CreateTableRequest.Builder requestBuilder = CreateTableRequest.builder()
                .tableName(tableName)
                .keySchema(keySchema)
                .attributeDefinitions(attributeDefinitions)
                .billingMode(BillingMode.PAY_PER_REQUEST);

        if (!gsiList.isEmpty()) {
            requestBuilder.globalSecondaryIndexes(gsiList);
            log.debug("Table {} will be created with {} GSI(s)", tableName, gsiList.size());
        }

        CreateTableRequest request = requestBuilder.build();

        try {
            dynamoDbClient.createTable(request);
            log.info("CreateTable request sent for: {}", tableName);
        } catch (ResourceInUseException e) {
            log.debug("Table {} already exists (concurrent creation), waiting for ACTIVE", tableName);
        } catch (DynamoException e) {
            // Don't double-wrap our own exceptions
            throw e;
        } catch (Exception e) {
            log.error("Failed to create table {}: {}", tableName, e.getMessage());
            throw new DynamoException(
                    "Failed to create table " + tableName + ": " + e.getMessage(), e);
        }

        try (DynamoDbWaiter waiter = DynamoDbWaiter.builder().client(dynamoDbClient).build()) {
            waiter.waitUntilTableExists(b -> b.tableName(tableName));
            log.info("Table {} is now ACTIVE", tableName);
        } catch (Exception e) {
            log.error("Failed waiting for table {} to become ACTIVE: {}", tableName, e.getMessage());
            throw new DynamoException(
                    "Failed waiting for table " + tableName + " to become ACTIVE: " + e.getMessage(), e);
        }
    }

    /**
     * Maps a Java field type to the corresponding DynamoDB scalar attribute type.
     * <ul>
     *   <li>String → S</li>
     *   <li>Number types (Integer, Long, Short, Byte, Float, Double, BigDecimal) → N</li>
     *   <li>byte[] → B</li>
     * </ul>
     *
     * @param fieldType the Java class of the key field
     * @return the DynamoDB ScalarAttributeType
     * @throws DynamoException if the type is not supported as a key type
     */
    static ScalarAttributeType toDynamoScalarType(Class<?> fieldType) {
        if (fieldType == String.class) {
            return ScalarAttributeType.S;
        }
        if (Number.class.isAssignableFrom(fieldType)
                || fieldType == int.class
                || fieldType == long.class
                || fieldType == short.class
                || fieldType == byte.class
                || fieldType == float.class
                || fieldType == double.class) {
            return ScalarAttributeType.N;
        }
        if (fieldType == byte[].class) {
            return ScalarAttributeType.B;
        }
        throw new DynamoException(
                "Unsupported key type: " + fieldType.getName()
                        + ". DynamoDB keys must be String (S), Number (N), or byte[] (B).");
    }
}
