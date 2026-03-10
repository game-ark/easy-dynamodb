package com.jojo.framework.easydynamodb.operation;

import com.jojo.framework.easydynamodb.exception.DynamoException;
import com.jojo.framework.easydynamodb.metadata.EntityMetadata;
import com.jojo.framework.easydynamodb.metadata.FieldMetadata;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.HashMap;
import java.util.Map;

/**
 * Shared utility for building DynamoDB key maps from entity metadata.
 * Eliminates duplicate key-building logic across GetOperation and BatchOperation.
 */
final class KeyBuilder {

    private KeyBuilder() {}

    /**
     * Builds a key map from partition key and optional sort key values.
     *
     * @param metadata     the entity metadata
     * @param partitionKey the partition key value
     * @param sortKey      the sort key value (nullable)
     * @return the key map for DynamoDB requests
     */
    static Map<String, AttributeValue> buildKeyMap(EntityMetadata metadata,
                                                    Object partitionKey,
                                                    Object sortKey) {
        Map<String, AttributeValue> keyMap = new HashMap<>();

        FieldMetadata pkField = metadata.getPartitionKey();
        keyMap.put(pkField.getDynamoAttributeName(), pkField.toAttributeValue(partitionKey));

        FieldMetadata skField = metadata.getSortKey();
        if (sortKey != null) {
            if (skField == null) {
                throw new DynamoException(
                        "Sort key provided but entity " + metadata.getEntityClass().getName()
                                + " does not define a @DynamoDbSortKey field");
            }
            keyMap.put(skField.getDynamoAttributeName(), skField.toAttributeValue(sortKey));
        } else if (skField != null) {
            throw new DynamoException(
                    "Entity " + metadata.getEntityClass().getName()
                            + " defines a @DynamoDbSortKey field '"
                            + skField.getJavaFieldName()
                            + "' but no sort key value was provided");
        }

        return keyMap;
    }

    /**
     * Extracts a human-readable key description from an item map for error reporting.
     */
    static String extractKeyDescription(Map<String, AttributeValue> item, EntityMetadata metadata) {
        FieldMetadata pk = metadata.getPartitionKey();
        AttributeValue pkValue = item.get(pk.getDynamoAttributeName());
        String desc = pk.getDynamoAttributeName() + "=" + (pkValue != null ? pkValue.toString() : "null");

        FieldMetadata sk = metadata.getSortKey();
        if (sk != null) {
            AttributeValue skValue = item.get(sk.getDynamoAttributeName());
            desc += ", " + sk.getDynamoAttributeName() + "=" + (skValue != null ? skValue.toString() : "null");
        }

        return desc;
    }
}
