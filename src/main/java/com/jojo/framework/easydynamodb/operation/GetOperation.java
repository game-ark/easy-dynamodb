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
 * 处理单实体获取操作，构建键映射、发起 GetItem 请求并将响应转换回 Java 实体。
 */
public class GetOperation {

    private static final DdmLogger log = DdmLogger.getLogger(GetOperation.class);

    private final DynamoDbClient dynamoDbClient;
    private final MetadataRegistry metadataRegistry;

    /**
     * Constructs a GetOperation.
     * 构造 GetOperation。
     *
     * @param dynamoDbClient   the DynamoDB client / DynamoDB 客户端
     * @param metadataRegistry the metadata registry / 元数据注册中心
     */
    public GetOperation(DynamoDbClient dynamoDbClient,
                        MetadataRegistry metadataRegistry) {
        this.dynamoDbClient = dynamoDbClient;
        this.metadataRegistry = metadataRegistry;
    }

    /**
     * Gets a single entity by partition key only (eventually consistent read).
     * 仅通过分区键获取单个实体（最终一致性读取）。
     *
     * @param clazz        the entity class / 实体类
     * @param partitionKey the partition key value / 分区键值
     * @param <T>          the entity type / 实体类型
     * @return the entity, or null if not found / 实体，未找到时返回 null
     */
    public <T> T get(Class<T> clazz, Object partitionKey) {
        return get(clazz, partitionKey, null, false);
    }

    /**
     * Gets a single entity by partition key and sort key (eventually consistent read).
     * 通过分区键和排序键获取单个实体（最终一致性读取）。
     *
     * @param clazz        the entity class / 实体类
     * @param partitionKey the partition key value / 分区键值
     * @param sortKey      the sort key value / 排序键值
     * @param <T>          the entity type / 实体类型
     * @return the entity, or null if not found / 实体，未找到时返回 null
     */
    public <T> T get(Class<T> clazz, Object partitionKey, Object sortKey) {
        return get(clazz, partitionKey, sortKey, false);
    }

    /**
     * Get a single entity by key with optional consistent read.
     * 通过键获取单个实体，支持可选的强一致性读取。
     *
     * @param clazz          the entity class / 实体类
     * @param partitionKey   the partition key value / 分区键值
     * @param sortKey        the sort key value (nullable) / 排序键值（可为 null）
     * @param consistentRead true for strongly consistent read, false for eventually consistent / true 为强一致性读取，false 为最终一致性读取
     * @param <T>            the entity type / 实体类型
     * @return the entity, or null if not found / 实体，未找到时返回 null
     */
    @SuppressWarnings("unchecked")
    public <T> T get(Class<T> clazz, Object partitionKey, Object sortKey, boolean consistentRead) {
        return get(clazz, partitionKey, sortKey, consistentRead, null);
    }

    /**
     * Get a single entity by key with optional consistent read and projection.
     * 通过键获取单个实体，支持可选的强一致性读取和投影表达式。
     * <p>
     * When a projection expression is specified, only the listed attributes are
     * returned from DynamoDB, reducing bandwidth and read capacity consumption.
     * Unmapped fields in the returned entity will be null.
     * 指定投影表达式时，仅从 DynamoDB 返回列出的属性，减少带宽和读取容量消耗。
     * 返回实体中未映射的字段将为 null。
     *
     * <pre>{@code
     * // Only fetch userId, nickName, and level
     * User user = ddm.get(User.class, "user-001", null, false, "userId, nickName, level");
     * }</pre>
     *
     * @param clazz               the entity class / 实体类
     * @param partitionKey        the partition key value / 分区键值
     * @param sortKey             the sort key value (nullable) / 排序键值（可为 null）
     * @param consistentRead      true for strongly consistent read / true 为强一致性读取
     * @param projectionExpression the projection expression (nullable — all attributes if null) / 投影表达式（可为 null，为 null 时返回所有属性）
     * @param <T>                 the entity type / 实体类型
     * @return the entity, or null if not found / 实体，未找到时返回 null
     */
    @SuppressWarnings("unchecked")
    public <T> T get(Class<T> clazz, Object partitionKey, Object sortKey,
                     boolean consistentRead, String projectionExpression) {
        metadataRegistry.register(clazz);
        EntityMetadata metadata = metadataRegistry.getMetadata(clazz);

        Map<String, AttributeValue> keyMap = KeyBuilder.buildKeyMap(metadata, partitionKey, sortKey);
        log.debug("GetItem from table={}, key={}, consistentRead={}, projection={}",
                metadata.getTableName(), keyMap, consistentRead,
                projectionExpression != null ? projectionExpression : "all");

        try {
            GetItemRequest.Builder requestBuilder = GetItemRequest.builder()
                    .tableName(metadata.getTableName())
                    .key(keyMap)
                    .consistentRead(consistentRead);

            if (projectionExpression != null && !projectionExpression.isEmpty()) {
                requestBuilder.projectionExpression(projectionExpression);
            }

            GetItemResponse response = dynamoDbClient.getItem(requestBuilder.build());

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
     * 将 DynamoDB 项（AttributeValue Map）转换为 Java 实体。
     * 此方法为 public，以便 BatchOperation 复用。
     *
     * @param item     the DynamoDB item map / DynamoDB 项映射
     * @param metadata the entity metadata / 实体元数据
     * @return the converted Java entity / 转换后的 Java 实体
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
