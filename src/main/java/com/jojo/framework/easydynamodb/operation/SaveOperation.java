package com.jojo.framework.easydynamodb.operation;

import com.jojo.framework.easydynamodb.exception.DynamoConditionFailedException;
import com.jojo.framework.easydynamodb.exception.DynamoException;
import com.jojo.framework.easydynamodb.logging.DdmLogger;
import com.jojo.framework.easydynamodb.metadata.EntityMetadata;
import com.jojo.framework.easydynamodb.metadata.FieldMetadata;
import com.jojo.framework.easydynamodb.metadata.MetadataRegistry;
import com.jojo.framework.easydynamodb.model.ConditionExpression;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Handles single-entity save operations by converting a Java entity to a
 * DynamoDB item (AttributeValue Map) and issuing a PutItem request.
 * 处理单实体保存操作，将 Java 实体转换为 DynamoDB 项（AttributeValue Map）并发起 PutItem 请求。
 */
public class SaveOperation {

    private static final DdmLogger log = DdmLogger.getLogger(SaveOperation.class);

    private final DynamoDbClient dynamoDbClient;
    private final MetadataRegistry metadataRegistry;
    private final boolean autoCreateTable;
    private final TableCreateOperation tableCreateOperation;

    /**
     * Constructs a SaveOperation with auto-create-table disabled.
     * 构造 SaveOperation，禁用自动建表功能。
     *
     * @param dynamoDbClient   the DynamoDB client / DynamoDB 客户端
     * @param metadataRegistry the metadata registry / 元数据注册中心
     */
    public SaveOperation(DynamoDbClient dynamoDbClient, MetadataRegistry metadataRegistry) {
        this(dynamoDbClient, metadataRegistry, false, null);
    }

    /**
     * Constructs a SaveOperation with optional auto-create-table support.
     * 构造 SaveOperation，支持可选的自动建表功能。
     *
     * @param dynamoDbClient       the DynamoDB client / DynamoDB 客户端
     * @param metadataRegistry     the metadata registry / 元数据注册中心
     * @param autoCreateTable      whether to auto-create the table if it doesn't exist / 表不存在时是否自动创建
     * @param tableCreateOperation the table creation operation (nullable if autoCreateTable is false) / 建表操作（autoCreateTable 为 false 时可为 null）
     */
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
     * 保存单个实体到 DynamoDB。如果实体类尚未注册到 MetadataRegistry，则自动注册。
     * <p>
     * When a {@link ResourceNotFoundException} is caught:
     * 当捕获到 {@link ResourceNotFoundException} 时：
     * <ul>
     *   <li>If autoCreateTable is false, throws a {@link DynamoException} indicating the table does not exist. / 如果 autoCreateTable 为 false，抛出 {@link DynamoException} 表示表不存在。</li>
     *   <li>If autoCreateTable is true, creates the table via {@link TableCreateOperation} and retries the save. / 如果 autoCreateTable 为 true，通过 {@link TableCreateOperation} 创建表并重试保存。</li>
     * </ul>
     *
     * @param entity the entity to save / 要保存的实体
     * @throws DynamoException if the DynamoDB operation fails / DynamoDB 操作失败时抛出
     */
    public void save(Object entity) {
        save(entity, null);
    }

    /**
     * Saves a single entity with a condition expression.
     * 保存单个实体，支持条件表达式。
     * <p>
     * The condition must evaluate to true for the save to succeed. If the condition
     * fails, a {@link DynamoConditionFailedException} is thrown.
     * 条件必须为 true 才能保存成功。如果条件不满足，将抛出 {@link DynamoConditionFailedException}。
     *
     * <pre>{@code
     * // Insert only if item doesn't exist
     * ddm.save(user, ConditionExpression.of("attribute_not_exists(userId)"));
     *
     * // Insert only if version matches (optimistic locking)
     * ddm.save(user, ConditionExpression.builder()
     *     .expression("#v = :expected")
     *     .name("#v", "version")
     *     .value(":expected", 3)
     *     .build());
     * }</pre>
     *
     * @param entity    the entity to save / 要保存的实体
     * @param condition the condition expression (nullable — no condition if null) / 条件表达式（可为 null，为 null 时不附加条件）
     * @throws DynamoConditionFailedException if the condition evaluates to false / 条件不满足时抛出
     * @throws DynamoException if the DynamoDB operation fails / DynamoDB 操作失败时抛出
     */
    public void save(Object entity, ConditionExpression condition) {
        Class<?> entityClass = entity.getClass();
        metadataRegistry.register(entityClass);
        EntityMetadata metadata = metadataRegistry.getMetadata(entityClass);

        Map<String, AttributeValue> item = toAttributeValueMap(entity, metadata);
        log.debug("PutItem to table={}, attributes={}, hasCondition={}",
                metadata.getTableName(), item.size(), condition != null);

        PutItemRequest.Builder requestBuilder = PutItemRequest.builder()
                .tableName(metadata.getTableName())
                .item(item);

        if (condition != null) {
            requestBuilder.conditionExpression(condition.getExpression());
            if (!condition.getExpressionNames().isEmpty()) {
                requestBuilder.expressionAttributeNames(condition.getExpressionNames());
            }
            if (!condition.getExpressionValues().isEmpty()) {
                requestBuilder.expressionAttributeValues(condition.getExpressionValues());
            }
        }

        PutItemRequest request = requestBuilder.build();

        try {
            dynamoDbClient.putItem(request);
            log.trace("PutItem succeeded for table={}", metadata.getTableName());
        } catch (ConditionalCheckFailedException e) {
            log.warn("PutItem condition failed for table={}: {}", metadata.getTableName(), e.getMessage());
            throw new DynamoConditionFailedException(
                    "Condition check failed for save on table "
                            + metadata.getTableName() + ": " + e.getMessage(), e);
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
     * 将实体转换为 {@code Map<String, AttributeValue>}，跳过值为 null 的字段。
     * 此方法为 public，以便 {@code BatchOperation} 复用。
     *
     * @param entity   the entity instance / 实体实例
     * @param metadata the entity's cached metadata / 实体的缓存元数据
     * @return a map of DynamoDB attribute names to attribute values / DynamoDB 属性名到属性值的映射
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
