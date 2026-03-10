package com.jojo.framework.easydynamodb.operation;

import com.jojo.framework.easydynamodb.exception.DynamoException;
import com.jojo.framework.easydynamodb.logging.DdmLogger;
import com.jojo.framework.easydynamodb.metadata.EntityMetadata;
import com.jojo.framework.easydynamodb.metadata.FieldMetadata;
import com.jojo.framework.easydynamodb.metadata.MetadataRegistry;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Handles single-entity save operations by converting a Java entity to a
 * DynamoDB item (AttributeValue Map) and issuing a PutItem request.
 */
public class SaveOperation {

    private static final DdmLogger log = DdmLogger.getLogger(SaveOperation.class);

    private final DynamoDbClient dynamoDbClient;
    private final MetadataRegistry metadataRegistry;
    private final boolean autoCreateTable;
    private final TableCreateOperation tableCreateOperation;

    public SaveOperation(DynamoDbClient dynamoDbClient, MetadataRegistry metadataRegistry) {
        this(dynamoDbClient, metadataRegistry, false, null);
    }

    public SaveOperation(DynamoDbClient dynamoDbClient, MetadataRegistry metadataRegistry,
                         boolean autoCreateTable, TableCreateOperation tableCreateOperation) {
        this.dynamoDbClient = dynamoDbClient;
        this.metadataRegistry = metadataRegistry;
        this.autoCreateTable = autoCreateTable;
        this.tableCreateOperation = tableCreateOperation;
    }

    /**
     * Saves a single entity to DynamoDB. The entity class is auto-registered
     * if not already present in the MetadataRegistry.
     * <p>
     * When a {@link ResourceNotFoundException} is caught:
     * <ul>
     *   <li>If autoCreateTable is false, throws a {@link DynamoException} indicating the table does not exist.</li>
     *   <li>If autoCreateTable is true, creates the table via {@link TableCreateOperation} and retries the save.</li>
     * </ul>
     *
     * @param entity the entity to save
     * @throws DynamoException if the DynamoDB operation fails
     */
    public void save(Object entity) {
        Class<?> entityClass = entity.getClass();
        metadataRegistry.register(entityClass);
        EntityMetadata metadata = metadataRegistry.getMetadata(entityClass);

        Map<String, AttributeValue> item = toAttributeValueMap(entity, metadata);
        log.debug("PutItem to table={}, attributes={}", metadata.getTableName(), item.size());

        PutItemRequest request = PutItemRequest.builder()
                .tableName(metadata.getTableName())
                .item(item)
                .build();

        try {
            dynamoDbClient.putItem(request);
            log.trace("PutItem succeeded for table={}", metadata.getTableName());
        } catch (ResourceNotFoundException e) {
            if (!autoCreateTable) {
                log.error("Table {} does not exist and autoCreateTable is disabled", metadata.getTableName());
                throw new DynamoException(
                        "Table " + metadata.getTableName() + " does not exist", e);
            }
            log.info("Table {} not found, auto-creating...", metadata.getTableName());
            tableCreateOperation.createTable(metadata);
            try {
                dynamoDbClient.putItem(request);
                log.info("PutItem succeeded after auto-creating table={}", metadata.getTableName());
            } catch (DynamoDbException retryEx) {
                log.error("PutItem failed after auto-creating table={}: {}", metadata.getTableName(), retryEx.getMessage());
                throw new DynamoException(
                        "Failed to save entity of type " + entityClass.getName()
                                + " to table " + metadata.getTableName()
                                + " after auto-creating table: " + retryEx.getMessage(), retryEx);
            }
        } catch (DynamoDbException e) {
            log.error("PutItem failed for table={}: {}", metadata.getTableName(), e.getMessage());
            throw new DynamoException(
                    "Failed to save entity of type " + entityClass.getName()
                            + " to table " + metadata.getTableName() + ": " + e.getMessage(), e);
        }
    }

    /**
     * Converts an entity to a {@code Map<String, AttributeValue>}, skipping
     * null-valued fields. This method is public so it can be reused by
     * {@code BatchOperation}.
     *
     * @param entity   the entity instance
     * @param metadata the entity's cached metadata
     * @return a map of DynamoDB attribute names to attribute values
     */
    public Map<String, AttributeValue> toAttributeValueMap(Object entity, EntityMetadata metadata) {
        Map<String, AttributeValue> item = new LinkedHashMap<>();

        for (FieldMetadata field : metadata.getFields()) {
            Object value = field.getValue(entity);
            if (value == null) {
                continue;
            }
            AttributeValue av = field.toAttributeValue(value);
            item.put(field.getDynamoAttributeName(), av);
        }

        return item;
    }
}
