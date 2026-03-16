package com.jojo.framework.easydynamodb.operation;

import com.jojo.framework.easydynamodb.exception.DynamoConditionFailedException;
import com.jojo.framework.easydynamodb.exception.DynamoException;
import com.jojo.framework.easydynamodb.logging.DdmLogger;
import com.jojo.framework.easydynamodb.metadata.EntityMetadata;
import com.jojo.framework.easydynamodb.metadata.FieldMetadata;
import com.jojo.framework.easydynamodb.metadata.MetadataRegistry;
import com.jojo.framework.easydynamodb.model.ConditionExpression;
import com.jojo.framework.easydynamodb.model.KeyPair;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Handles delete operations: single delete by key, and conditional delete
 * via query-then-delete (scan + filter → batch delete, returns count).
 * 处理删除操作：按键单条删除，以及通过查询后删除（扫描 + 过滤 → 批量删除，返回删除数量）的条件删除。
 */
public class DeleteOperation {

    private static final DdmLogger log = DdmLogger.getLogger(DeleteOperation.class);
    private static final int MAX_RETRIES = 3;
    private static final long INITIAL_BACKOFF_MS = 100;

    private final DynamoDbClient dynamoDbClient;
    private final MetadataRegistry metadataRegistry;

    /**
     * Constructs a DeleteOperation.
     * 构造 DeleteOperation。
     *
     * @param dynamoDbClient   the DynamoDB client / DynamoDB 客户端
     * @param metadataRegistry the metadata registry / 元数据注册中心
     */
    public DeleteOperation(DynamoDbClient dynamoDbClient, MetadataRegistry metadataRegistry) {
        this.dynamoDbClient = dynamoDbClient;
        this.metadataRegistry = metadataRegistry;
    }

    /**
     * Deletes a single entity by partition key only.
     * 仅通过分区键删除单个实体。
     *
     * @param clazz        the entity class / 实体类
     * @param partitionKey the partition key value / 分区键值
     * @param <T>          the entity type / 实体类型
     */
    public <T> void delete(Class<T> clazz, Object partitionKey) {
        delete(clazz, partitionKey, null, null);
    }

    /**
     * Deletes a single entity by partition key and sort key.
     * 通过分区键和排序键删除单个实体。
     *
     * @param clazz        the entity class / 实体类
     * @param partitionKey the partition key value / 分区键值
     * @param sortKey      the sort key value / 排序键值
     * @param <T>          the entity type / 实体类型
     */
    public <T> void delete(Class<T> clazz, Object partitionKey, Object sortKey) {
        delete(clazz, partitionKey, sortKey, null);
    }

    /**
     * Deletes a single entity with a condition expression.
     * 删除单个实体，支持条件表达式。
     *
     * <pre>{@code
     * // Delete only if status is "INACTIVE"
     * ddm.delete(User.class, "user-001", null,
     *     ConditionExpression.builder()
     *         .expression("#status = :expected")
     *         .name("#status", "status")
     *         .value(":expected", "INACTIVE")
     *         .build());
     * }</pre>
     *
     * @param clazz        the entity class / 实体类
     * @param partitionKey the partition key value / 分区键值
     * @param sortKey      the sort key value (nullable) / 排序键值（可为 null）
     * @param condition    the condition expression (nullable) / 条件表达式（可为 null）
     * @param <T>          the entity type / 实体类型
     * @throws DynamoConditionFailedException if the condition evaluates to false / 条件不满足时抛出
     */
    public <T> void delete(Class<T> clazz, Object partitionKey, Object sortKey, ConditionExpression condition) {
        metadataRegistry.register(clazz);
        EntityMetadata metadata = metadataRegistry.getMetadata(clazz);

        Map<String, AttributeValue> keyMap = KeyBuilder.buildKeyMap(metadata, partitionKey, sortKey);
        log.debug("DeleteItem from table={}, key={}, hasCondition={}",
                metadata.getTableName(), keyMap, condition != null);

        try {
            DeleteItemRequest.Builder builder = DeleteItemRequest.builder()
                    .tableName(metadata.getTableName())
                    .key(keyMap);

            if (condition != null) {
                builder.conditionExpression(condition.getExpression());
                if (!condition.getExpressionNames().isEmpty()) {
                    builder.expressionAttributeNames(condition.getExpressionNames());
                }
                if (!condition.getExpressionValues().isEmpty()) {
                    builder.expressionAttributeValues(condition.getExpressionValues());
                }
            }

            dynamoDbClient.deleteItem(builder.build());
            log.trace("DeleteItem succeeded for table={}", metadata.getTableName());
        } catch (ConditionalCheckFailedException e) {
            log.warn("DeleteItem condition failed for table={}: {}", metadata.getTableName(), e.getMessage());
            throw new DynamoConditionFailedException(
                    "Condition check failed for delete on table "
                            + metadata.getTableName() + ": " + e.getMessage(), e);
        } catch (DynamoDbException e) {
            log.error("DeleteItem failed for table={}: {}", metadata.getTableName(), e.getMessage());
            throw new DynamoException(
                    "Failed to delete entity of type " + clazz.getName()
                            + " from table " + metadata.getTableName() + ": " + e.getMessage(), e);
        }
    }

    /**
     * Deletes all items matching a condition by scanning with a filter expression,
     * extracting keys, and batch-deleting them. Returns the number of items deleted.
     * 通过扫描过滤表达式匹配的所有项，提取键并批量删除。返回已删除的项数。
     * <p>
     * DynamoDB does not support "DELETE WHERE condition" natively, so this method
     * performs a scan → extract keys → batch delete loop internally.
     * DynamoDB 原生不支持 "DELETE WHERE condition"，因此此方法内部执行 扫描 → 提取键 → 批量删除 循环。
     *
     * @param clazz            the entity class / 实体类
     * @param filterExpression the filter expression (e.g. "rating < :minRating") / 过滤表达式（例如 "rating < :minRating"）
     * @param expressionValues the expression attribute values / 表达式属性值
     * @param expressionNames  the expression attribute names (nullable) / 表达式属性名（可为 null）
     * @param <T>              the entity type / 实体类型
     * @return the number of items successfully deleted / 成功删除的项数
     */
    public <T> int deleteByCondition(Class<T> clazz,
                                     String filterExpression,
                                     Map<String, AttributeValue> expressionValues,
                                     Map<String, String> expressionNames) {
        metadataRegistry.register(clazz);
        EntityMetadata metadata = metadataRegistry.getMetadata(clazz);
        String tableName = metadata.getTableName();

        log.debug("deleteByCondition on table={}, filter={}", tableName, filterExpression);

        int totalDeleted = 0;
        Map<String, AttributeValue> exclusiveStartKey = null;

        // Build projection to only fetch key attributes (saves bandwidth)
        FieldMetadata pk = metadata.getPartitionKey();
        FieldMetadata sk = metadata.getSortKey();
        String pkAttr = pk.getDynamoAttributeName();
        String skAttr = sk != null ? sk.getDynamoAttributeName() : null;

        do {
            ScanRequest.Builder scanBuilder = ScanRequest.builder()
                    .tableName(tableName)
                    .filterExpression(filterExpression);

            if (expressionValues != null && !expressionValues.isEmpty()) {
                scanBuilder.expressionAttributeValues(expressionValues);
            }
            if (expressionNames != null && !expressionNames.isEmpty()) {
                scanBuilder.expressionAttributeNames(expressionNames);
            }
            if (exclusiveStartKey != null) {
                scanBuilder.exclusiveStartKey(exclusiveStartKey);
            }

            // Only project key fields to minimize data transfer
            if (skAttr != null) {
                scanBuilder.projectionExpression(pkAttr + ", " + skAttr);
            } else {
                scanBuilder.projectionExpression(pkAttr);
            }

            ScanResponse response;
            try {
                response = dynamoDbClient.scan(scanBuilder.build());
            } catch (DynamoDbException e) {
                log.error("Scan for conditional delete failed on table={}: {}", tableName, e.getMessage());
                throw new DynamoException(
                        "Failed to scan for conditional delete on table " + tableName + ": " + e.getMessage(), e);
            }

            if (response.hasItems() && !response.items().isEmpty()) {
                List<KeyPair> keysToDelete = new ArrayList<>();
                for (Map<String, AttributeValue> item : response.items()) {
                    Object pkValue = pk.fromAttributeValue(item.get(pkAttr));
                    Object skValue = (sk != null && item.containsKey(skAttr))
                            ? sk.fromAttributeValue(item.get(skAttr)) : null;
                    keysToDelete.add(new KeyPair(pkValue, skValue));
                }

                log.debug("deleteByCondition scan page returned {} items for table={}", keysToDelete.size(), tableName);

                // Batch delete in chunks of 25 with retry
                for (int i = 0; i < keysToDelete.size(); i += 25) {
                    List<KeyPair> chunk = keysToDelete.subList(i, Math.min(i + 25, keysToDelete.size()));
                    List<WriteRequest> writeRequests = new ArrayList<>();
                    for (KeyPair kp : chunk) {
                        Map<String, AttributeValue> keyMap = KeyBuilder.buildKeyMap(metadata, kp.partitionKey(), kp.sortKey());
                        writeRequests.add(WriteRequest.builder()
                                .deleteRequest(DeleteRequest.builder().key(keyMap).build())
                                .build());
                    }

                    try {
                        int deletedInChunk = executeBatchDeleteWithRetry(tableName, writeRequests, chunk.size());
                        totalDeleted += deletedInChunk;
                        log.trace("deleteByCondition batch deleted {} items from table={}, totalDeleted={}",
                                deletedInChunk, tableName, totalDeleted);
                    } catch (DynamoDbException e) {
                        log.error("Batch delete in deleteByCondition failed on table={} after deleting {} items: {}",
                                tableName, totalDeleted, e.getMessage());
                        throw new DynamoException(
                                "Failed to batch delete on table " + tableName + ": " + e.getMessage(), e);
                    }
                }
            } else {
                log.trace("deleteByCondition scan page returned 0 items for table={}", tableName);
            }

            exclusiveStartKey = response.hasLastEvaluatedKey() ? response.lastEvaluatedKey() : null;
        } while (exclusiveStartKey != null && !exclusiveStartKey.isEmpty());

        log.info("deleteByCondition completed: deleted {} items from table={}", totalDeleted, tableName);
        return totalDeleted;
    }

    /**
     * Executes a batch delete with retry for unprocessed items.
     * Returns the number of items actually deleted.
     */
    private int executeBatchDeleteWithRetry(String tableName, List<WriteRequest> writeRequests, int totalInChunk) {
        Map<String, List<WriteRequest>> remaining = Map.of(tableName, writeRequests);

        for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
            BatchWriteItemResponse response = dynamoDbClient.batchWriteItem(
                    BatchWriteItemRequest.builder()
                            .requestItems(remaining)
                            .build());

            if (!response.hasUnprocessedItems() || response.unprocessedItems().isEmpty()) {
                return totalInChunk;
            }

            List<WriteRequest> unprocessed = response.unprocessedItems().get(tableName);
            if (unprocessed == null || unprocessed.isEmpty()) {
                return totalInChunk;
            }

            int unprocessedCount = unprocessed.size();
            remaining = Map.of(tableName, unprocessed);

            if (attempt < MAX_RETRIES) {
                log.warn("deleteByCondition: {} unprocessed items, retrying (attempt {}/{})",
                        unprocessedCount, attempt + 1, MAX_RETRIES);
                backoff(attempt);
            } else {
                log.warn("deleteByCondition: {} items remained unprocessed after {} retries on table={}",
                        unprocessedCount, MAX_RETRIES, tableName);
                return totalInChunk - unprocessedCount;
            }
        }

        return totalInChunk;
    }

    private void backoff(int attempt) {
        try {
            long sleepMs = INITIAL_BACKOFF_MS * (1L << attempt);
            Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
