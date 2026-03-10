package com.jojo.framework.easydynamodb.operation;

import com.jojo.framework.easydynamodb.exception.DynamoException;
import com.jojo.framework.easydynamodb.logging.DdmLogger;
import com.jojo.framework.easydynamodb.metadata.EntityMetadata;
import com.jojo.framework.easydynamodb.metadata.FieldMetadata;
import com.jojo.framework.easydynamodb.metadata.MetadataRegistry;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;

import java.util.Map;

/**
 * Handles single-entity get operations by building a key map, issuing a
 * GetItem request, and converting the response back to a Java entity.
 */
public class GetOperation {

    private static final DdmLogger log = DdmLogger.getLogger(GetOperation.class);

    private final DynamoDbClient dynamoDbClient;
    private final MetadataRegistry metadataRegistry;

    public GetOperation(DynamoDbClient dynamoDbClient,
                        MetadataRegistry metadataRegistry) {
        this.dynamoDbClient = dynamoDbClient;
        this.metadataRegistry = metadataRegistry;
    }

    public <T> T get(Class<T> clazz, Object partitionKey) {
        return get(clazz, partitionKey, null);
    }

    @SuppressWarnings("unchecked")
    public <T> T get(Class<T> clazz, Object partitionKey, Object sortKey) {
        metadataRegistry.register(clazz);
        EntityMetadata metadata = metadataRegistry.getMetadata(clazz);

        Map<String, AttributeValue> keyMap = KeyBuilder.buildKeyMap(metadata, partitionKey, sortKey);
        log.debug("GetItem from table={}, key={}", metadata.getTableName(), keyMap);

        try {
            GetItemResponse response = dynamoDbClient.getItem(GetItemRequest.builder()
                    .tableName(metadata.getTableName())
                    .key(keyMap)
                    .build());

            if (!response.hasItem() || response.item().isEmpty()) {
                log.debug("GetItem returned no item for table={}", metadata.getTableName());
                return null;
            }

            Map<String, AttributeValue> rawItem = response.item();
            log.trace("GetItem raw response: {} attributes", rawItem.size());
            return (T) fromAttributeValueMap(rawItem, metadata);
        } catch (DynamoDbException e) {
            log.error("GetItem failed for table={}: {}", metadata.getTableName(), e.getMessage());
            throw new DynamoException(
                    "Failed to get entity of type " + clazz.getName()
                            + " from table " + metadata.getTableName() + ": " + e.getMessage(), e);
        }
    }

    /**
     * Converts a DynamoDB item (AttributeValue Map) to a Java entity.
     * Public so it can be reused by BatchOperation.
     */
    public Object fromAttributeValueMap(Map<String, AttributeValue> item, EntityMetadata metadata) {
        Object entity = metadata.newInstance();

        for (FieldMetadata field : metadata.getFields()) {
            String attrName = field.getDynamoAttributeName();
            AttributeValue av = item.get(attrName);
            if (av != null) {
                Object value = field.fromAttributeValue(av);
                field.setValue(entity, value);
            }
        }

        return entity;
    }


}
