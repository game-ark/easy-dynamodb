package com.jojo.framework.easydynamodb.operation;

import com.jojo.framework.easydynamodb.exception.DynamoException;
import com.jojo.framework.easydynamodb.metadata.EntityMetadata;
import com.jojo.framework.easydynamodb.metadata.FieldMetadata;
import com.jojo.framework.easydynamodb.metadata.MetadataRegistry;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Handles partial and full update operations using DynamoDB UpdateExpression (SET).
 */
public class UpdateOperation {

    private final DynamoDbClient dynamoDbClient;
    private final MetadataRegistry metadataRegistry;

    public UpdateOperation(DynamoDbClient dynamoDbClient, MetadataRegistry metadataRegistry) {
        this.dynamoDbClient = dynamoDbClient;
        this.metadataRegistry = metadataRegistry;
    }

    /**
     * Partial update: creates a clean entity with only PK/SK copied from the
     * source entity, passes it to the mutator so the user can set fields to
     * update, then sends only those non-null non-key fields as a SET expression.
     * If no non-key fields are set, the request is skipped entirely.
     */
    @SuppressWarnings("unchecked")
    public <T> void update(T entity, Consumer<T> mutator) {
        Class<?> entityClass = entity.getClass();
        metadataRegistry.register(entityClass);
        EntityMetadata metadata = metadataRegistry.getMetadata(entityClass);

        // 1. Create a clean instance with only key values
        T cleanEntity = (T) metadata.newInstance();
        FieldMetadata pkField = metadata.getPartitionKey();
        pkField.setValue(cleanEntity, pkField.getValue(entity));

        FieldMetadata skField = metadata.getSortKey();
        if (skField != null) {
            skField.setValue(cleanEntity, skField.getValue(entity));
        }

        // 2. Let the user set fields they want to update
        mutator.accept(cleanEntity);

        // 3. Build SET expression from non-null non-key fields
        sendUpdate(cleanEntity, metadata);
    }

    /**
     * Full update: iterates all non-null non-key fields of the given entity
     * and sends them as a SET expression.
     */
    public <T> void updateAll(T entity) {
        Class<?> entityClass = entity.getClass();
        metadataRegistry.register(entityClass);
        EntityMetadata metadata = metadataRegistry.getMetadata(entityClass);

        sendUpdate(entity, metadata);
    }

    // ---- Internal helpers ----

    private void sendUpdate(Object entity, EntityMetadata metadata) {
        Map<String, AttributeValue> keyMap = new HashMap<>();
        Map<String, AttributeValue> expressionValues = new HashMap<>();
        Map<String, String> expressionNames = new HashMap<>();
        StringBuilder setExpression = new StringBuilder("SET ");
        int fieldIndex = 0;

        for (FieldMetadata field : metadata.getFields()) {
            if (field.isPartitionKey()) {
                keyMap.put(field.getDynamoAttributeName(),
                        field.toAttributeValue(field.getValue(entity)));
                continue;
            }
            if (field.isSortKey()) {
                keyMap.put(field.getDynamoAttributeName(),
                        field.toAttributeValue(field.getValue(entity)));
                continue;
            }

            Object value = field.getValue(entity);
            if (value == null) {
                continue;
            }

            String nameAlias = "#f" + fieldIndex;
            String valueAlias = ":v" + fieldIndex;
            expressionNames.put(nameAlias, field.getDynamoAttributeName());
            expressionValues.put(valueAlias, field.toAttributeValue(value));

            if (fieldIndex > 0) {
                setExpression.append(", ");
            }
            setExpression.append(nameAlias).append(" = ").append(valueAlias);
            fieldIndex++;
        }

        // If no non-key fields to update, skip the request
        if (fieldIndex == 0) {
            return;
        }

        try {
            dynamoDbClient.updateItem(UpdateItemRequest.builder()
                    .tableName(metadata.getTableName())
                    .key(keyMap)
                    .updateExpression(setExpression.toString())
                    .expressionAttributeNames(expressionNames)
                    .expressionAttributeValues(expressionValues)
                    .build());
        } catch (DynamoDbException e) {
            throw new DynamoException(
                    "Failed to update entity of type " + metadata.getEntityClass().getName()
                            + " in table " + metadata.getTableName() + ": " + e.getMessage(), e);
        }
    }
}
