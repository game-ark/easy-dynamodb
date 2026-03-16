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
 * 用于从实体元数据构建 DynamoDB 键映射的共享工具类。
 * 消除 GetOperation 和 BatchOperation 之间重复的键构建逻辑。
 */
final class KeyBuilder {

    private KeyBuilder() {}

    /**
     * Builds a key map from partition key and optional sort key values.
     * 根据分区键和可选排序键值构建键映射。
     *
     * @param metadata     the entity metadata / 实体元数据
     * @param partitionKey the partition key value / 分区键值
     * @param sortKey      the sort key value (nullable) / 排序键值（可为 null）
     * @return the key map for DynamoDB requests / 用于 DynamoDB 请求的键映射
     * @throws DynamoException if sort key is provided but not defined, or vice versa / 提供了排序键但未定义时抛出，反之亦然
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
     * 从项映射中提取人类可读的键描述，用于错误报告。
     *
     * @param item     the DynamoDB item map / DynamoDB 项映射
     * @param metadata the entity metadata / 实体元数据
     * @return a human-readable key description string / 人类可读的键描述字符串
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
