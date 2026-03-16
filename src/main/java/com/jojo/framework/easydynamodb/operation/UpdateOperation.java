package com.jojo.framework.easydynamodb.operation;

import com.jojo.framework.easydynamodb.exception.DynamoBatchException;
import com.jojo.framework.easydynamodb.exception.DynamoBatchException.BatchFailure;
import com.jojo.framework.easydynamodb.exception.DynamoConditionFailedException;
import com.jojo.framework.easydynamodb.exception.DynamoException;
import com.jojo.framework.easydynamodb.logging.DdmLogger;
import com.jojo.framework.easydynamodb.metadata.EntityMetadata;
import com.jojo.framework.easydynamodb.metadata.FieldMetadata;
import com.jojo.framework.easydynamodb.metadata.MetadataRegistry;
import com.jojo.framework.easydynamodb.model.ConditionExpression;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * Handles partial and full update operations using DynamoDB UpdateExpression.
 * 处理使用 DynamoDB UpdateExpression 的部分更新和全量更新操作。
 * <p>
 * Supports both SET (assign value) and REMOVE (delete attribute) expressions.
 * In partial update mode, a change-tracking proxy detects which fields the
 * mutator touched — including fields explicitly set to {@code null}, which
 * generates a REMOVE expression to delete the attribute from DynamoDB.
 * 支持 SET（赋值）和 REMOVE（删除属性）表达式。在部分更新模式下，变更追踪代理检测
 * mutator 修改了哪些字段——包括显式设置为 {@code null} 的字段，这会生成 REMOVE 表达式
 * 以从 DynamoDB 中删除该属性。
 */
public class UpdateOperation {

    private static final DdmLogger log = DdmLogger.getLogger(UpdateOperation.class);
    private static final Executor DEFAULT_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

    private final DynamoDbClient dynamoDbClient;
    private final MetadataRegistry metadataRegistry;
    private final Executor executor;

    /**
     * Constructs an UpdateOperation with the default virtual-thread executor.
     * 使用默认虚拟线程执行器构造 UpdateOperation。
     *
     * @param dynamoDbClient   the DynamoDB client / DynamoDB 客户端
     * @param metadataRegistry the metadata registry / 元数据注册中心
     */
    public UpdateOperation(DynamoDbClient dynamoDbClient, MetadataRegistry metadataRegistry) {
        this(dynamoDbClient, metadataRegistry, DEFAULT_EXECUTOR);
    }

    /**
     * Constructs an UpdateOperation with a custom executor.
     * 使用自定义执行器构造 UpdateOperation。
     *
     * @param dynamoDbClient   the DynamoDB client / DynamoDB 客户端
     * @param metadataRegistry the metadata registry / 元数据注册中心
     * @param executor         the executor for parallel batch updates (nullable, defaults to virtual threads) / 用于并行批量更新的执行器（可为 null，默认使用虚拟线程）
     */
    public UpdateOperation(DynamoDbClient dynamoDbClient, MetadataRegistry metadataRegistry, Executor executor) {
        this.dynamoDbClient = dynamoDbClient;
        this.metadataRegistry = metadataRegistry;
        this.executor = executor != null ? executor : DEFAULT_EXECUTOR;
    }

    /**
     * Partial update: creates a clean entity, tracks which fields the mutator
     * touches (including explicit null assignments), then sends SET for non-null
     * values and REMOVE for null values.
     * 部分更新：创建一个干净的实体，追踪 mutator 修改了哪些字段（包括显式设置为 null），
     * 然后对非 null 值发送 SET，对 null 值发送 REMOVE。
     *
     * @param entity  the entity (must contain valid primary key values) / 实体（必须包含有效的主键值）
     * @param mutator the mutation to apply / 要应用的变更操作
     * @param <T>     the entity type / 实体类型
     */
    @SuppressWarnings("unchecked")
    public <T> void update(T entity, Consumer<T> mutator) {
        update(entity, mutator, null);
    }

    /**
     * Partial update with a condition expression.
     * 带条件表达式的部分更新。
     * <p>
     * The condition must evaluate to true for the update to succeed.
     * 条件必须为 true 才能更新成功。
     *
     * <pre>{@code
     * // Update coins only if current value matches (optimistic locking)
     * ddm.update(user, u -> u.setCoins(newCoins),
     *     ConditionExpression.builder()
     *         .expression("#coins = :expected")
     *         .name("#coins", "coins")
     *         .value(":expected", oldCoins)
     *         .build());
     * }</pre>
     *
     * @param entity    the entity (must contain valid primary key values) / 实体（必须包含有效的主键值）
     * @param mutator   the mutation to apply / 要应用的变更操作
     * @param condition the condition expression (nullable) / 条件表达式（可为 null）
     * @param <T>       the entity type / 实体类型
     * @throws DynamoConditionFailedException if the condition evaluates to false / 条件不满足时抛出
     */
    @SuppressWarnings("unchecked")
    public <T> void update(T entity, Consumer<T> mutator, ConditionExpression condition) {
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

        // 2. Snapshot all field values before mutator runs
        Map<String, Object> beforeValues = new HashMap<>();
        for (FieldMetadata field : metadata.getFields()) {
            if (field.isPartitionKey() || field.isSortKey()) continue;
            beforeValues.put(field.getJavaFieldName(), field.getValue(cleanEntity));
        }

        // 3. Let the user set fields they want to update
        mutator.accept(cleanEntity);

        // 4. Detect which fields changed (including explicit null assignments)
        Set<String> changedFields = new HashSet<>();
        for (FieldMetadata field : metadata.getFields()) {
            if (field.isPartitionKey() || field.isSortKey()) continue;
            Object before = beforeValues.get(field.getJavaFieldName());
            Object after = field.getValue(cleanEntity);
            if (!Objects.equals(before, after)) {
                changedFields.add(field.getJavaFieldName());
            }
        }

        if (changedFields.isEmpty()) {
            log.debug("Partial update skipped for {}: no fields changed", entityClass.getSimpleName());
            return;
        }

        log.debug("Partial update for {}: changed fields={}", entityClass.getSimpleName(), changedFields);

        // 5. Build and send update with SET + REMOVE
        buildAndSendUpdate(cleanEntity, metadata, changedFields, condition);
    }

    /**
     * Full update: iterates all non-key fields of the given entity.
     * Non-null fields become SET expressions; null fields become REMOVE expressions.
     * 全量更新：遍历给定实体的所有非键字段。非 null 字段生成 SET 表达式；null 字段生成 REMOVE 表达式。
     *
     * @param entity the entity to fully update / 要全量更新的实体
     * @param <T>    the entity type / 实体类型
     */
    public <T> void updateAll(T entity) {
        updateAll(entity, null);
    }

    /**
     * Full update with a condition expression.
     * 带条件表达式的全量更新。
     *
     * @param entity    the entity to fully update / 要全量更新的实体
     * @param condition the condition expression (nullable) / 条件表达式（可为 null）
     * @param <T>       the entity type / 实体类型
     * @throws DynamoConditionFailedException if the condition evaluates to false / 条件不满足时抛出
     */
    public <T> void updateAll(T entity, ConditionExpression condition) {
        Class<?> entityClass = entity.getClass();
        metadataRegistry.register(entityClass);
        EntityMetadata metadata = metadataRegistry.getMetadata(entityClass);

        buildAndSendUpdate(entity, metadata, null, condition);
    }

    /**
     * Batch update: applies the same mutator to each entity and sends individual
     * UpdateItem requests. DynamoDB does not support batch UpdateItem natively,
     * so this method parallelizes individual updates using virtual threads.
     * 批量更新：对每个实体应用相同的 mutator 并发送单独的 UpdateItem 请求。
     * DynamoDB 原生不支持批量 UpdateItem，因此此方法使用虚拟线程并行化单独的更新。
     *
     * @param entities the entities to update (must contain valid primary key values) / 要更新的实体列表（必须包含有效的主键值）
     * @param mutator  the mutation to apply to each entity / 要应用到每个实体的变更操作
     * @param <T>      the entity type / 实体类型
     */
    @SuppressWarnings("unchecked")
    public <T> void updateBatch(List<T> entities, Consumer<T> mutator) {
        executeBatchUpdate(entities, entity -> update(entity, mutator), "partial");
    }

    /**
     * Batch full update: sends updateAll for each entity in parallel.
     * 批量全量更新：并行对每个实体执行 updateAll。
     *
     * @param entities the entities to fully update / 要全量更新的实体列表
     * @param <T>      the entity type / 实体类型
     */
    public <T> void updateAllBatch(List<T> entities) {
        executeBatchUpdate(entities, this::updateAll, "full");
    }

    /**
     * Shared batch update executor. Parallelizes individual update calls using virtual threads,
     * collects errors, and throws if any updates failed.
     *
     * @param entities  the entities to update
     * @param action    the update action to apply to each entity
     * @param opName    operation name for logging ("partial" or "full")
     */
    private <T> void executeBatchUpdate(List<T> entities, Consumer<T> action, String opName) {
        if (entities == null || entities.isEmpty()) {
            log.debug("Batch {} update skipped: empty entity list", opName);
            return;
        }

        Class<?> entityClass = entities.get(0).getClass();
        metadataRegistry.register(entityClass);

        log.debug("Batch {} update: {} entities of type {}", opName, entities.size(), entityClass.getSimpleName());

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        List<BatchFailure> failures = new CopyOnWriteArrayList<>();

        EntityMetadata metadata = metadataRegistry.getMetadata(entityClass);

        for (T entity : entities) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    action.accept(entity);
                } catch (DynamoException e) {
                    log.warn("Batch {} update: single entity failed for {}: {}",
                            opName, entityClass.getSimpleName(), e.getMessage());
                    // Extract key description for the failure report
                    String keyDesc = extractKeyDescription(entity, metadata);
                    failures.add(new BatchFailure(keyDesc, e.getMessage()));
                }
            }, executor));
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof DynamoException de) throw de;
            throw new DynamoException("Batch " + opName + " update failed: " + cause.getMessage(), cause);
        }

        if (!failures.isEmpty()) {
            log.error("Batch {} update completed with {}/{} errors for {}",
                    opName, failures.size(), entities.size(), entityClass.getSimpleName());
            throw new DynamoBatchException(failures);
        }

        log.trace("Batch {} update completed successfully for {} entities of {}",
                opName, entities.size(), entityClass.getSimpleName());
    }

    // ---- Internal helpers ----

    /**
     * Unified update expression builder. When {@code fieldsToInclude} is non-null,
     * only those fields are considered (partial update); when null, all non-key
     * fields are included (full update).
     */
    private void buildAndSendUpdate(Object entity, EntityMetadata metadata,
                                     Set<String> fieldsToInclude, ConditionExpression condition) {
        Map<String, AttributeValue> keyMap = new HashMap<>();
        Map<String, AttributeValue> expressionValues = new HashMap<>();
        Map<String, String> expressionNames = new HashMap<>();
        StringBuilder setExpression = new StringBuilder();
        StringBuilder removeExpression = new StringBuilder();
        int setIndex = 0;
        int removeIndex = 0;

        for (FieldMetadata field : metadata.getFields()) {
            if (field.isPartitionKey() || field.isSortKey()) {
                keyMap.put(field.getDynamoAttributeName(),
                        field.toAttributeValue(field.getValue(entity)));
                continue;
            }

            // If partial update, skip fields not in the change set
            if (fieldsToInclude != null && !fieldsToInclude.contains(field.getJavaFieldName())) {
                continue;
            }

            Object value = field.getValue(entity);
            String nameAlias = "#f" + (setIndex + removeIndex);
            expressionNames.put(nameAlias, field.getDynamoAttributeName());

            if (value != null) {
                String valueAlias = ":v" + setIndex;
                expressionValues.put(valueAlias, field.toAttributeValue(value));
                if (setIndex > 0) setExpression.append(", ");
                setExpression.append(nameAlias).append(" = ").append(valueAlias);
                setIndex++;
            } else {
                if (removeIndex > 0) removeExpression.append(", ");
                removeExpression.append(nameAlias);
                removeIndex++;
            }
        }

        if (setIndex == 0 && removeIndex == 0) {
            log.debug("Update skipped for {}: no SET or REMOVE expressions", metadata.getEntityClass().getSimpleName());
            return;
        }

        StringBuilder updateExpression = new StringBuilder();
        if (setIndex > 0) {
            updateExpression.append("SET ").append(setExpression);
        }
        if (removeIndex > 0) {
            if (setIndex > 0) updateExpression.append(" ");
            updateExpression.append("REMOVE ").append(removeExpression);
        }

        executeUpdate(metadata, keyMap, updateExpression.toString(), expressionNames, expressionValues, condition);
    }

    private void executeUpdate(EntityMetadata metadata,
                               Map<String, AttributeValue> keyMap,
                               String updateExpression,
                               Map<String, String> expressionNames,
                               Map<String, AttributeValue> expressionValues,
                               ConditionExpression condition) {
        log.debug("UpdateItem table={}, expression={}, hasCondition={}",
                metadata.getTableName(), updateExpression, condition != null);

        // Merge condition expression names/values
        Map<String, String> allNames = new HashMap<>(expressionNames);
        Map<String, AttributeValue> allValues = new HashMap<>(expressionValues);
        if (condition != null) {
            allNames.putAll(condition.getExpressionNames());
            allValues.putAll(condition.getExpressionValues());
        }

        try {
            UpdateItemRequest.Builder builder = UpdateItemRequest.builder()
                    .tableName(metadata.getTableName())
                    .key(keyMap)
                    .updateExpression(updateExpression);

            if (!allNames.isEmpty()) {
                builder.expressionAttributeNames(allNames);
            }

            // DynamoDB rejects empty expressionAttributeValues
            if (!allValues.isEmpty()) {
                builder.expressionAttributeValues(allValues);
            }

            if (condition != null) {
                builder.conditionExpression(condition.getExpression());
            }

            dynamoDbClient.updateItem(builder.build());
            log.trace("UpdateItem succeeded for table={}", metadata.getTableName());
        } catch (ConditionalCheckFailedException e) {
            log.warn("UpdateItem condition failed for table={}: {}", metadata.getTableName(), e.getMessage());
            throw new DynamoConditionFailedException(
                    "Condition check failed for update on table "
                            + metadata.getTableName() + ": " + e.getMessage(), e);
        } catch (DynamoDbException e) {
            log.error("UpdateItem failed for table={}: {}", metadata.getTableName(), e.getMessage());
            throw new DynamoException(
                    "Failed to update entity of type " + metadata.getEntityClass().getName()
                            + " in table " + metadata.getTableName() + ": " + e.getMessage(), e);
        }
    }

    /**
     * Extracts a human-readable key description from an entity for error reporting.
     */
    private <T> String extractKeyDescription(T entity, EntityMetadata metadata) {
        try {
            FieldMetadata pk = metadata.getPartitionKey();
            Object pkValue = pk.getValue(entity);
            String desc = pk.getDynamoAttributeName() + "=" + pkValue;
            FieldMetadata sk = metadata.getSortKey();
            if (sk != null) {
                Object skValue = sk.getValue(entity);
                desc += ", " + sk.getDynamoAttributeName() + "=" + skValue;
            }
            return desc;
        } catch (Exception e) {
            return "unknown-key";
        }
    }
}
