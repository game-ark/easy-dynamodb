package com.jojo.framework.easydynamodb.operation;

import com.jojo.framework.easydynamodb.exception.DynamoConditionFailedException;
import com.jojo.framework.easydynamodb.exception.DynamoException;
import com.jojo.framework.easydynamodb.exception.DynamoTransactionException;
import com.jojo.framework.easydynamodb.logging.DdmLogger;
import com.jojo.framework.easydynamodb.metadata.EntityMetadata;
import com.jojo.framework.easydynamodb.metadata.FieldMetadata;
import com.jojo.framework.easydynamodb.metadata.MetadataRegistry;
import com.jojo.framework.easydynamodb.model.ConditionExpression;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.*;
import java.util.function.Consumer;

/**
 * Handles DynamoDB TransactWriteItems and TransactGetItems operations.
 * <p>
 * Supports up to 100 items per transaction (DynamoDB limit), with automatic
 * splitting into multiple transactions if needed (opt-in).
 * <p>
 * A transaction guarantees all-or-nothing atomicity: either all items succeed
 * or none are applied. This is essential for operations like:
 * <ul>
 *   <li>Transferring currency between two players</li>
 *   <li>Deducting items while granting rewards</li>
 *   <li>Creating related records across multiple tables atomically</li>
 * </ul>
 *
 * <pre>{@code
 * ddm.transact()
 *     .put(newUser)
 *     .put(newWallet, c -> c.condition("attribute_not_exists(userId)"))
 *     .update(User.class, "user-001", u -> u
 *         .increment("coins", 100)
 *         .condition("#coins >= :min", cc -> cc.name("#coins", "coins").value(":min", 0)))
 *     .delete(TempRecord.class, "temp-001")
 *     .conditionCheck(Inventory.class, "user-001", "item-sword",
 *         ConditionExpression.builder()
 *             .expression("#count >= :required")
 *             .name("#count", "count")
 *             .value(":required", 1)
 *             .build())
 *     .execute();
 * }</pre>
 */
public class TransactionOperation {

    private static final DdmLogger log = DdmLogger.getLogger(TransactionOperation.class);
    private static final int MAX_TRANSACTION_ITEMS = 100;

    private final DynamoDbClient dynamoDbClient;
    private final MetadataRegistry metadataRegistry;
    private final SaveOperation saveOperation;

    public TransactionOperation(DynamoDbClient dynamoDbClient,
                                MetadataRegistry metadataRegistry,
                                SaveOperation saveOperation) {
        this.dynamoDbClient = dynamoDbClient;
        this.metadataRegistry = metadataRegistry;
        this.saveOperation = saveOperation;
    }

    /**
     * Creates a new transaction write builder.
     *
     * @return a new TransactWriteBuilder
     */
    public TransactWriteBuilder transact() {
        return new TransactWriteBuilder();
    }

    /**
     * Creates a new transaction read builder.
     *
     * @return a new TransactGetBuilder
     */
    public TransactGetBuilder transactGet() {
        return new TransactGetBuilder();
    }

    // ========================================================================
    // TransactWriteItems Builder
    // ========================================================================

    /**
     * Fluent builder for constructing DynamoDB TransactWriteItems requests.
     * <p>
     * Supports four action types per item:
     * <ul>
     *   <li>{@code put} — insert or replace an item</li>
     *   <li>{@code update} — expression-based update</li>
     *   <li>{@code delete} — remove an item</li>
     *   <li>{@code conditionCheck} — validate a condition without modifying data</li>
     * </ul>
     */
    public class TransactWriteBuilder {
        private final List<TransactWriteItem> items = new ArrayList<>();
        private String clientRequestToken;

        // ---- Put ----

        /**
         * Adds a Put action to the transaction.
         *
         * @param entity the entity to put
         * @return this builder
         */
        public <T> TransactWriteBuilder put(T entity) {
            return put(entity, null);
        }

        /**
         * Adds a Put action with a condition expression.
         *
         * @param entity    the entity to put
         * @param condition the condition that must be met (nullable)
         * @return this builder
         */
        public <T> TransactWriteBuilder put(T entity, ConditionExpression condition) {
            Class<?> entityClass = entity.getClass();
            metadataRegistry.register(entityClass);
            EntityMetadata metadata = metadataRegistry.getMetadata(entityClass);

            Map<String, AttributeValue> item = saveOperation.toAttributeValueMap(entity, metadata);

            Put.Builder putBuilder = Put.builder()
                    .tableName(metadata.getTableName())
                    .item(item);

            applyCondition(putBuilder, condition);

            items.add(TransactWriteItem.builder().put(putBuilder.build()).build());
            return this;
        }

        // ---- Update (expression-based) ----

        /**
         * Adds an expression-based Update action to the transaction.
         * <p>
         * The configurator receives an {@link TransactUpdateBuilder} to build
         * the update expression with SET/REMOVE/ADD/DELETE clauses and conditions.
         *
         * @param clazz        the entity class
         * @param partitionKey the partition key value
         * @param configurator callback to configure the update expression
         * @return this builder
         */
        public <T> TransactWriteBuilder update(Class<T> clazz, Object partitionKey,
                                                Consumer<TransactUpdateBuilder> configurator) {
            return update(clazz, partitionKey, null, configurator);
        }

        /**
         * Adds an expression-based Update action with composite key.
         *
         * @param clazz        the entity class
         * @param partitionKey the partition key value
         * @param sortKey      the sort key value (nullable)
         * @param configurator callback to configure the update expression
         * @return this builder
         */
        public <T> TransactWriteBuilder update(Class<T> clazz, Object partitionKey, Object sortKey,
                                                Consumer<TransactUpdateBuilder> configurator) {
            metadataRegistry.register(clazz);
            EntityMetadata metadata = metadataRegistry.getMetadata(clazz);

            TransactUpdateBuilder updateBuilder = new TransactUpdateBuilder(metadata, partitionKey, sortKey);
            configurator.accept(updateBuilder);

            items.add(TransactWriteItem.builder().update(updateBuilder.build()).build());
            return this;
        }

        // ---- Delete ----

        /**
         * Adds a Delete action to the transaction.
         *
         * @param clazz        the entity class
         * @param partitionKey the partition key value
         * @return this builder
         */
        public <T> TransactWriteBuilder delete(Class<T> clazz, Object partitionKey) {
            return delete(clazz, partitionKey, null, null);
        }

        /**
         * Adds a Delete action with composite key.
         *
         * @param clazz        the entity class
         * @param partitionKey the partition key value
         * @param sortKey      the sort key value
         * @return this builder
         */
        public <T> TransactWriteBuilder delete(Class<T> clazz, Object partitionKey, Object sortKey) {
            return delete(clazz, partitionKey, sortKey, null);
        }

        /**
         * Adds a Delete action with condition expression.
         *
         * @param clazz        the entity class
         * @param partitionKey the partition key value
         * @param sortKey      the sort key value (nullable)
         * @param condition    the condition expression (nullable)
         * @return this builder
         */
        public <T> TransactWriteBuilder delete(Class<T> clazz, Object partitionKey,
                                                Object sortKey, ConditionExpression condition) {
            metadataRegistry.register(clazz);
            EntityMetadata metadata = metadataRegistry.getMetadata(clazz);
            Map<String, AttributeValue> keyMap = KeyBuilder.buildKeyMap(metadata, partitionKey, sortKey);

            Delete.Builder deleteBuilder = Delete.builder()
                    .tableName(metadata.getTableName())
                    .key(keyMap);

            applyCondition(deleteBuilder, condition);

            items.add(TransactWriteItem.builder().delete(deleteBuilder.build()).build());
            return this;
        }

        // ---- ConditionCheck ----

        /**
         * Adds a ConditionCheck action — validates a condition without modifying data.
         * <p>
         * Useful for ensuring preconditions are met before the transaction commits.
         *
         * @param clazz        the entity class
         * @param partitionKey the partition key value
         * @param condition    the condition that must be satisfied
         * @return this builder
         */
        public <T> TransactWriteBuilder conditionCheck(Class<T> clazz, Object partitionKey,
                                                        ConditionExpression condition) {
            return conditionCheck(clazz, partitionKey, null, condition);
        }

        /**
         * Adds a ConditionCheck action with composite key.
         *
         * @param clazz        the entity class
         * @param partitionKey the partition key value
         * @param sortKey      the sort key value (nullable)
         * @param condition    the condition that must be satisfied
         * @return this builder
         */
        public <T> TransactWriteBuilder conditionCheck(Class<T> clazz, Object partitionKey,
                                                        Object sortKey, ConditionExpression condition) {
            if (condition == null) {
                throw new DynamoException("ConditionCheck requires a non-null condition expression");
            }

            metadataRegistry.register(clazz);
            EntityMetadata metadata = metadataRegistry.getMetadata(clazz);
            Map<String, AttributeValue> keyMap = KeyBuilder.buildKeyMap(metadata, partitionKey, sortKey);

            ConditionCheck.Builder checkBuilder = ConditionCheck.builder()
                    .tableName(metadata.getTableName())
                    .key(keyMap)
                    .conditionExpression(condition.getExpression());

            if (!condition.getExpressionNames().isEmpty()) {
                checkBuilder.expressionAttributeNames(condition.getExpressionNames());
            }
            if (!condition.getExpressionValues().isEmpty()) {
                checkBuilder.expressionAttributeValues(condition.getExpressionValues());
            }

            items.add(TransactWriteItem.builder().conditionCheck(checkBuilder.build()).build());
            return this;
        }

        // ---- Idempotency ----

        /**
         * Sets a client request token for idempotency.
         * <p>
         * If the same token is used within 10 minutes, DynamoDB returns the
         * previous result without re-executing the transaction.
         *
         * @param token the idempotency token
         * @return this builder
         */
        public TransactWriteBuilder idempotencyToken(String token) {
            this.clientRequestToken = token;
            return this;
        }

        // ---- Execute ----

        /**
         * Executes the transaction. All items succeed or none are applied.
         *
         * @throws DynamoTransactionException if the transaction is cancelled
         * @throws DynamoConditionFailedException if a condition check fails
         * @throws DynamoException if the transaction fails for other reasons
         */
        public void execute() {
            if (items.isEmpty()) {
                throw new DynamoException("Transaction must contain at least one item");
            }
            if (items.size() > MAX_TRANSACTION_ITEMS) {
                throw new DynamoException(
                        "Transaction contains " + items.size() + " items, exceeding the DynamoDB limit of "
                                + MAX_TRANSACTION_ITEMS + ". Split into multiple transactions.");
            }

            log.debug("TransactWriteItems: {} items", items.size());

            TransactWriteItemsRequest.Builder requestBuilder = TransactWriteItemsRequest.builder()
                    .transactItems(items);

            if (clientRequestToken != null) {
                requestBuilder.clientRequestToken(clientRequestToken);
            }

            try {
                dynamoDbClient.transactWriteItems(requestBuilder.build());
                log.trace("TransactWriteItems succeeded with {} items", items.size());
            } catch (TransactionCanceledException e) {
                List<String> reasons = new ArrayList<>();
                if (e.hasCancellationReasons()) {
                    for (CancellationReason reason : e.cancellationReasons()) {
                        String code = reason.code();
                        String msg = reason.message();
                        reasons.add(code != null ? code + ": " + (msg != null ? msg : "") : "None");
                    }
                }

                log.warn("TransactWriteItems cancelled: reasons={}", reasons);

                // Check if it's specifically a condition failure
                boolean hasConditionFailure = reasons.stream()
                        .anyMatch(r -> r.startsWith("ConditionalCheckFailed"));
                if (hasConditionFailure) {
                    throw new DynamoConditionFailedException(
                            "Transaction cancelled due to condition check failure: " + reasons, e);
                }

                throw new DynamoTransactionException(
                        "Transaction cancelled with " + items.size() + " items: " + reasons, reasons, e);
            } catch (IdempotentParameterMismatchException e) {
                log.error("TransactWriteItems idempotency mismatch: {}", e.getMessage());
                throw new DynamoTransactionException(
                        "Transaction idempotency token mismatch: " + e.getMessage(), e);
            } catch (DynamoDbException e) {
                log.error("TransactWriteItems failed: {}", e.getMessage());
                throw new DynamoTransactionException(
                        "Transaction failed: " + e.getMessage(), e);
            }
        }

        // ---- Internal helpers ----

        private void applyCondition(Put.Builder builder, ConditionExpression condition) {
            if (condition == null) return;
            builder.conditionExpression(condition.getExpression());
            if (!condition.getExpressionNames().isEmpty()) {
                builder.expressionAttributeNames(condition.getExpressionNames());
            }
            if (!condition.getExpressionValues().isEmpty()) {
                builder.expressionAttributeValues(condition.getExpressionValues());
            }
        }

        private void applyCondition(Delete.Builder builder, ConditionExpression condition) {
            if (condition == null) return;
            builder.conditionExpression(condition.getExpression());
            if (!condition.getExpressionNames().isEmpty()) {
                builder.expressionAttributeNames(condition.getExpressionNames());
            }
            if (!condition.getExpressionValues().isEmpty()) {
                builder.expressionAttributeValues(condition.getExpressionValues());
            }
        }
    }

    // ========================================================================
    // TransactUpdateBuilder (used inside TransactWriteBuilder)
    // ========================================================================

    /**
     * Builder for constructing an Update action within a transaction.
     * Mirrors the expression capabilities of {@link ExpressionUpdateOperation.ExpressionUpdateBuilder}
     * but produces a DynamoDB {@link Update} object for use in transactions.
     */
    public static class TransactUpdateBuilder {
        private final EntityMetadata metadata;
        private final Object partitionKey;
        private final Object sortKey;

        private final List<String> setClauses = new ArrayList<>();
        private final List<String> removeClauses = new ArrayList<>();
        private final List<String> addClauses = new ArrayList<>();
        private final List<String> deleteClauses = new ArrayList<>();
        private final Map<String, String> expressionNames = new HashMap<>();
        private final Map<String, AttributeValue> expressionValues = new HashMap<>();
        private ConditionExpression conditionExpression;

        TransactUpdateBuilder(EntityMetadata metadata, Object partitionKey, Object sortKey) {
            this.metadata = metadata;
            this.partitionKey = partitionKey;
            this.sortKey = sortKey;
        }

        public TransactUpdateBuilder set(String setExpression) {
            setClauses.add(setExpression);
            return this;
        }

        public TransactUpdateBuilder set(String attributeName, Object value) {
            String nameAlias = "#sa" + setClauses.size();
            String valueAlias = ":sv" + setClauses.size();
            expressionNames.put(nameAlias, attributeName);
            expressionValues.put(valueAlias, AttributeValues.of(value));
            setClauses.add(nameAlias + " = " + valueAlias);
            return this;
        }

        public TransactUpdateBuilder increment(String attributeName, Number amount) {
            String nameAlias = "#inc" + setClauses.size();
            String valueAlias = ":inc" + setClauses.size();
            expressionNames.put(nameAlias, attributeName);
            expressionValues.put(valueAlias, AttributeValues.of(amount));
            setClauses.add(nameAlias + " = " + nameAlias + " + " + valueAlias);
            return this;
        }

        public TransactUpdateBuilder decrement(String attributeName, Number amount) {
            String nameAlias = "#dec" + setClauses.size();
            String valueAlias = ":dec" + setClauses.size();
            expressionNames.put(nameAlias, attributeName);
            expressionValues.put(valueAlias, AttributeValues.of(amount));
            setClauses.add(nameAlias + " = " + nameAlias + " - " + valueAlias);
            return this;
        }

        public TransactUpdateBuilder remove(String... attributeNames) {
            for (String attr : attributeNames) {
                String nameAlias = "#rm" + removeClauses.size();
                expressionNames.put(nameAlias, attr);
                removeClauses.add(nameAlias);
            }
            return this;
        }

        public TransactUpdateBuilder add(String attributeName, Object value) {
            String nameAlias = "#ad" + addClauses.size();
            String valueAlias = ":ad" + addClauses.size();
            expressionNames.put(nameAlias, attributeName);
            expressionValues.put(valueAlias, AttributeValues.of(value));
            addClauses.add(nameAlias + " " + valueAlias);
            return this;
        }

        public TransactUpdateBuilder name(String placeholder, String attributeName) {
            expressionNames.put(placeholder, attributeName);
            return this;
        }

        public TransactUpdateBuilder value(String placeholder, Object val) {
            expressionValues.put(placeholder, AttributeValues.of(val));
            return this;
        }

        public TransactUpdateBuilder rawValue(String placeholder, AttributeValue attributeValue) {
            expressionValues.put(placeholder, attributeValue);
            return this;
        }

        public TransactUpdateBuilder condition(ConditionExpression condition) {
            this.conditionExpression = condition;
            return this;
        }

        public TransactUpdateBuilder condition(String expression) {
            this.conditionExpression = ConditionExpression.of(expression);
            return this;
        }

        public TransactUpdateBuilder condition(String expression,
                                                Consumer<ConditionExpression.Builder> configurator) {
            ConditionExpression.Builder builder = ConditionExpression.builder().expression(expression);
            configurator.accept(builder);
            this.conditionExpression = builder.build();
            return this;
        }

        Update build() {
            if (setClauses.isEmpty() && removeClauses.isEmpty()
                    && addClauses.isEmpty() && deleteClauses.isEmpty()) {
                throw new DynamoException("Transaction update requires at least one SET, REMOVE, ADD, or DELETE clause");
            }

            Map<String, AttributeValue> keyMap = KeyBuilder.buildKeyMap(metadata, partitionKey, sortKey);

            StringBuilder updateExpression = new StringBuilder();
            if (!setClauses.isEmpty()) {
                updateExpression.append("SET ").append(String.join(", ", setClauses));
            }
            if (!removeClauses.isEmpty()) {
                if (!updateExpression.isEmpty()) updateExpression.append(" ");
                updateExpression.append("REMOVE ").append(String.join(", ", removeClauses));
            }
            if (!addClauses.isEmpty()) {
                if (!updateExpression.isEmpty()) updateExpression.append(" ");
                updateExpression.append("ADD ").append(String.join(", ", addClauses));
            }
            if (!deleteClauses.isEmpty()) {
                if (!updateExpression.isEmpty()) updateExpression.append(" ");
                updateExpression.append("DELETE ").append(String.join(", ", deleteClauses));
            }

            Map<String, String> allNames = new HashMap<>(expressionNames);
            Map<String, AttributeValue> allValues = new HashMap<>(expressionValues);
            if (conditionExpression != null) {
                allNames.putAll(conditionExpression.getExpressionNames());
                allValues.putAll(conditionExpression.getExpressionValues());
            }

            Update.Builder builder = Update.builder()
                    .tableName(metadata.getTableName())
                    .key(keyMap)
                    .updateExpression(updateExpression.toString());

            if (!allNames.isEmpty()) {
                builder.expressionAttributeNames(allNames);
            }
            if (!allValues.isEmpty()) {
                builder.expressionAttributeValues(allValues);
            }
            if (conditionExpression != null) {
                builder.conditionExpression(conditionExpression.getExpression());
            }

            return builder.build();
        }
    }

    // ========================================================================
    // TransactGetItems Builder
    // ========================================================================

    /**
     * Fluent builder for constructing DynamoDB TransactGetItems requests.
     * <p>
     * Reads up to 100 items atomically — guarantees a consistent snapshot
     * across all items at the time of the read.
     *
     * <pre>{@code
     * TransactGetResult result = ddm.transactGet()
     *     .get(User.class, "user-001")
     *     .get(Wallet.class, "user-001")
     *     .get(Inventory.class, "user-001", "item-sword")
     *     .execute();
     *
     * User user = result.get(0, User.class);
     * Wallet wallet = result.get(1, Wallet.class);
     * Inventory inv = result.get(2, Inventory.class);
     * }</pre>
     */
    public class TransactGetBuilder {
        private final List<TransactGetItem> getItems = new ArrayList<>();
        private final List<Class<?>> entityClasses = new ArrayList<>();

        /**
         * Adds a Get action by partition key.
         *
         * @param clazz        the entity class
         * @param partitionKey the partition key value
         * @return this builder
         */
        public <T> TransactGetBuilder get(Class<T> clazz, Object partitionKey) {
            return get(clazz, partitionKey, null);
        }

        /**
         * Adds a Get action by composite key.
         *
         * @param clazz        the entity class
         * @param partitionKey the partition key value
         * @param sortKey      the sort key value (nullable)
         * @return this builder
         */
        public <T> TransactGetBuilder get(Class<T> clazz, Object partitionKey, Object sortKey) {
            metadataRegistry.register(clazz);
            EntityMetadata metadata = metadataRegistry.getMetadata(clazz);
            Map<String, AttributeValue> keyMap = KeyBuilder.buildKeyMap(metadata, partitionKey, sortKey);

            Get get = Get.builder()
                    .tableName(metadata.getTableName())
                    .key(keyMap)
                    .build();

            getItems.add(TransactGetItem.builder().get(get).build());
            entityClasses.add(clazz);
            return this;
        }

        /**
         * Adds a Get action with projection (only return specific attributes).
         *
         * @param clazz               the entity class
         * @param partitionKey         the partition key value
         * @param sortKey              the sort key value (nullable)
         * @param projectionExpression the projection expression
         * @return this builder
         */
        public <T> TransactGetBuilder get(Class<T> clazz, Object partitionKey, Object sortKey,
                                           String projectionExpression) {
            metadataRegistry.register(clazz);
            EntityMetadata metadata = metadataRegistry.getMetadata(clazz);
            Map<String, AttributeValue> keyMap = KeyBuilder.buildKeyMap(metadata, partitionKey, sortKey);

            Get.Builder getBuilder = Get.builder()
                    .tableName(metadata.getTableName())
                    .key(keyMap);

            if (projectionExpression != null && !projectionExpression.isEmpty()) {
                getBuilder.projectionExpression(projectionExpression);
            }

            getItems.add(TransactGetItem.builder().get(getBuilder.build()).build());
            entityClasses.add(clazz);
            return this;
        }

        /**
         * Executes the transactional get. Returns a consistent snapshot of all items.
         *
         * @return the transaction get result
         * @throws DynamoTransactionException if the transaction fails
         */
        public TransactGetResult execute() {
            if (getItems.isEmpty()) {
                throw new DynamoException("TransactGet must contain at least one item");
            }
            if (getItems.size() > MAX_TRANSACTION_ITEMS) {
                throw new DynamoException(
                        "TransactGet contains " + getItems.size() + " items, exceeding the DynamoDB limit of "
                                + MAX_TRANSACTION_ITEMS);
            }

            log.debug("TransactGetItems: {} items", getItems.size());

            try {
                TransactGetItemsResponse response = dynamoDbClient.transactGetItems(
                        TransactGetItemsRequest.builder()
                                .transactItems(getItems)
                                .build());

                List<Map<String, AttributeValue>> rawItems = new ArrayList<>();
                if (response.hasResponses()) {
                    for (ItemResponse itemResponse : response.responses()) {
                        rawItems.add(itemResponse.hasItem() ? itemResponse.item() : null);
                    }
                }

                log.trace("TransactGetItems succeeded with {} items", rawItems.size());
                return new TransactGetResult(rawItems, entityClasses, metadataRegistry);
            } catch (TransactionCanceledException e) {
                log.error("TransactGetItems cancelled: {}", e.getMessage());
                throw new DynamoTransactionException(
                        "TransactGet cancelled: " + e.getMessage(), e);
            } catch (DynamoDbException e) {
                log.error("TransactGetItems failed: {}", e.getMessage());
                throw new DynamoTransactionException(
                        "TransactGet failed: " + e.getMessage(), e);
            }
        }
    }

    // ========================================================================
    // TransactGetResult
    // ========================================================================

    /**
     * Result of a TransactGetItems operation. Items are returned in the same
     * order as they were added to the builder.
     */
    public static class TransactGetResult {
        private final List<Map<String, AttributeValue>> rawItems;
        private final List<Class<?>> entityClasses;
        private final MetadataRegistry metadataRegistry;

        TransactGetResult(List<Map<String, AttributeValue>> rawItems,
                          List<Class<?>> entityClasses,
                          MetadataRegistry metadataRegistry) {
            this.rawItems = rawItems;
            this.entityClasses = entityClasses;
            this.metadataRegistry = metadataRegistry;
        }

        /**
         * Gets the item at the specified index, converted to the expected entity type.
         *
         * @param index the 0-based index (same order as builder calls)
         * @param clazz the expected entity class
         * @return the entity, or null if the item was not found
         */
        @SuppressWarnings("unchecked")
        public <T> T get(int index, Class<T> clazz) {
            if (index < 0 || index >= rawItems.size()) {
                throw new DynamoException("Index " + index + " out of bounds for transaction result of size " + rawItems.size());
            }
            Map<String, AttributeValue> item = rawItems.get(index);
            if (item == null || item.isEmpty()) {
                return null;
            }
            EntityMetadata metadata = metadataRegistry.getMetadata(clazz);
            return (T) convertFromMap(item, metadata);
        }

        /**
         * Returns the number of items in the result.
         */
        public int size() {
            return rawItems.size();
        }

        private Object convertFromMap(Map<String, AttributeValue> item, EntityMetadata metadata) {
            Object entity = metadata.newInstance();
            for (FieldMetadata field : metadata.getFields()) {
                AttributeValue av = item.get(field.getDynamoAttributeName());
                if (av != null) {
                    field.setValue(entity, field.fromAttributeValue(av));
                }
            }
            return entity;
        }
    }
}
