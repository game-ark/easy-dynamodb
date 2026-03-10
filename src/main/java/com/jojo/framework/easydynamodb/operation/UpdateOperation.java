package com.jojo.framework.easydynamodb.operation;

import com.jojo.framework.easydynamodb.exception.DynamoException;
import com.jojo.framework.easydynamodb.logging.DdmLogger;
import com.jojo.framework.easydynamodb.metadata.EntityMetadata;
import com.jojo.framework.easydynamodb.metadata.FieldMetadata;
import com.jojo.framework.easydynamodb.metadata.MetadataRegistry;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * Handles partial and full update operations using DynamoDB UpdateExpression.
 * <p>
 * Supports both SET (assign value) and REMOVE (delete attribute) expressions.
 * In partial update mode, a change-tracking proxy detects which fields the
 * mutator touched — including fields explicitly set to {@code null}, which
 * generates a REMOVE expression to delete the attribute from DynamoDB.
 */
public class UpdateOperation {

    private static final DdmLogger log = DdmLogger.getLogger(UpdateOperation.class);
    private static final Executor BATCH_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

    private final DynamoDbClient dynamoDbClient;
    private final MetadataRegistry metadataRegistry;

    public UpdateOperation(DynamoDbClient dynamoDbClient, MetadataRegistry metadataRegistry) {
        this.dynamoDbClient = dynamoDbClient;
        this.metadataRegistry = metadataRegistry;
    }

    /**
     * Partial update: creates a clean entity, tracks which fields the mutator
     * touches (including explicit null assignments), then sends SET for non-null
     * values and REMOVE for null values.
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
            if (!java.util.Objects.equals(before, after)) {
                changedFields.add(field.getJavaFieldName());
            }
        }

        if (changedFields.isEmpty()) {
            log.debug("Partial update skipped for {}: no fields changed", entityClass.getSimpleName());
            return;
        }

        log.debug("Partial update for {}: changed fields={}", entityClass.getSimpleName(), changedFields);

        // 5. Build and send update with SET + REMOVE
        buildAndSendUpdate(cleanEntity, metadata, changedFields);
    }

    /**
     * Full update: iterates all non-key fields of the given entity.
     * Non-null fields become SET expressions; null fields become REMOVE expressions.
     */
    public <T> void updateAll(T entity) {
        Class<?> entityClass = entity.getClass();
        metadataRegistry.register(entityClass);
        EntityMetadata metadata = metadataRegistry.getMetadata(entityClass);

        buildAndSendUpdate(entity, metadata, null);
    }

    /**
     * Batch update: applies the same mutator to each entity and sends individual
     * UpdateItem requests. DynamoDB does not support batch UpdateItem natively,
     * so this method parallelizes individual updates using virtual threads.
     *
     * @param entities the entities to update (must contain valid primary key values)
     * @param mutator  the mutation to apply to each entity
     */
    @SuppressWarnings("unchecked")
    public <T> void updateBatch(List<T> entities, Consumer<T> mutator) {
        if (entities == null || entities.isEmpty()) {
            log.debug("Batch partial update skipped: empty entity list");
            return;
        }

        Class<?> entityClass = entities.get(0).getClass();
        metadataRegistry.register(entityClass);
        EntityMetadata metadata = metadataRegistry.getMetadata(entityClass);

        log.debug("Batch partial update: {} entities of type {}", entities.size(), entityClass.getSimpleName());

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        List<DynamoException> errors = new CopyOnWriteArrayList<>();

        for (T entity : entities) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    update(entity, mutator);
                } catch (DynamoException e) {
                    log.warn("Batch partial update: single entity failed for {}: {}",
                            entityClass.getSimpleName(), e.getMessage());
                    errors.add(e);
                }
            }, BATCH_EXECUTOR));
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } catch (java.util.concurrent.CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof DynamoException de) throw de;
            throw new DynamoException("Batch update failed: " + cause.getMessage(), cause);
        }

        if (!errors.isEmpty()) {
            log.error("Batch partial update completed with {}/{} errors for {}",
                    errors.size(), entities.size(), entityClass.getSimpleName());
            throw new DynamoException(
                    "Batch update completed with " + errors.size() + " error(s). First: " + errors.get(0).getMessage(),
                    errors.get(0));
        }

        log.trace("Batch partial update completed successfully for {} entities of {}",
                entities.size(), entityClass.getSimpleName());
    }

    /**
     * Batch full update: sends updateAll for each entity in parallel.
     *
     * @param entities the entities to fully update
     */
    public <T> void updateAllBatch(List<T> entities) {
        if (entities == null || entities.isEmpty()) {
            log.debug("Batch full update skipped: empty entity list");
            return;
        }

        Class<?> entityClass = entities.get(0).getClass();
        metadataRegistry.register(entityClass);

        log.debug("Batch full update: {} entities of type {}", entities.size(), entityClass.getSimpleName());

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        List<DynamoException> errors = new CopyOnWriteArrayList<>();

        for (T entity : entities) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    updateAll(entity);
                } catch (DynamoException e) {
                    log.warn("Batch full update: single entity failed for {}: {}",
                            entityClass.getSimpleName(), e.getMessage());
                    errors.add(e);
                }
            }, BATCH_EXECUTOR));
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } catch (java.util.concurrent.CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof DynamoException de) throw de;
            throw new DynamoException("Batch updateAll failed: " + cause.getMessage(), cause);
        }

        if (!errors.isEmpty()) {
            log.error("Batch full update completed with {}/{} errors for {}",
                    errors.size(), entities.size(), entityClass.getSimpleName());
            throw new DynamoException(
                    "Batch updateAll completed with " + errors.size() + " error(s). First: " + errors.get(0).getMessage(),
                    errors.get(0));
        }

        log.trace("Batch full update completed successfully for {} entities of {}",
                entities.size(), entityClass.getSimpleName());
    }

    // ---- Internal helpers ----

    /**
     * Unified update expression builder. When {@code fieldsToInclude} is non-null,
     * only those fields are considered (partial update); when null, all non-key
     * fields are included (full update).
     */
    private void buildAndSendUpdate(Object entity, EntityMetadata metadata, Set<String> fieldsToInclude) {
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

        executeUpdate(metadata, keyMap, updateExpression.toString(), expressionNames, expressionValues);
    }

    private void executeUpdate(EntityMetadata metadata,
                               Map<String, AttributeValue> keyMap,
                               String updateExpression,
                               Map<String, String> expressionNames,
                               Map<String, AttributeValue> expressionValues) {
        log.debug("UpdateItem table={}, expression={}", metadata.getTableName(), updateExpression);
        try {
            UpdateItemRequest.Builder builder = UpdateItemRequest.builder()
                    .tableName(metadata.getTableName())
                    .key(keyMap)
                    .updateExpression(updateExpression)
                    .expressionAttributeNames(expressionNames);

            // DynamoDB rejects empty expressionAttributeValues
            if (!expressionValues.isEmpty()) {
                builder.expressionAttributeValues(expressionValues);
            }

            dynamoDbClient.updateItem(builder.build());
            log.trace("UpdateItem succeeded for table={}", metadata.getTableName());
        } catch (DynamoDbException e) {
            log.error("UpdateItem failed for table={}: {}", metadata.getTableName(), e.getMessage());
            throw new DynamoException(
                    "Failed to update entity of type " + metadata.getEntityClass().getName()
                            + " in table " + metadata.getTableName() + ": " + e.getMessage(), e);
        }
    }
}
