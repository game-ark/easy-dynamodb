package com.jojo.framework.easydynamodb;

import com.jojo.framework.easydynamodb.converter.AttributeConverter;
import com.jojo.framework.easydynamodb.converter.ConverterRegistry;
import com.jojo.framework.easydynamodb.logging.DdmLogger;
import com.jojo.framework.easydynamodb.metadata.MetadataRegistry;
import com.jojo.framework.easydynamodb.metadata.TableNameResolver;
import com.jojo.framework.easydynamodb.model.ConditionExpression;
import com.jojo.framework.easydynamodb.model.KeyPair;
import com.jojo.framework.easydynamodb.operation.*;
import org.slf4j.event.Level;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * DynamodbDataManager — the core entry point for all DynamoDB operations.
 * <p>
 * Use {@link #create(DynamoDbClient)} for quick setup with defaults, or
 * {@link #builder(DynamoDbClient)} for fine-grained configuration.
 */
public class DDM {

    private static final DdmLogger log = DdmLogger.getLogger(DDM.class);

    private final DynamoDbClient client;
    private final MetadataRegistry metadataRegistry;
    private final ConverterRegistry converterRegistry;
    private final boolean autoCreateTable;
    private final SaveOperation saveOperation;
    private final GetOperation getOperation;
    private final UpdateOperation updateOperation;
    private final DeleteOperation deleteOperation;
    private final BatchOperation batchOperation;
    private final QueryOperation queryOperation;
    private final ScanOperation scanOperation;
    private final ExpressionUpdateOperation expressionUpdateOperation;
    private final TransactionOperation transactionOperation;

    private DDM(DynamoDbClient client, MetadataRegistry metadataRegistry,
                ConverterRegistry converterRegistry, boolean autoCreateTable,
                SaveOperation saveOperation, GetOperation getOperation,
                UpdateOperation updateOperation, DeleteOperation deleteOperation,
                BatchOperation batchOperation, QueryOperation queryOperation,
                ScanOperation scanOperation,
                ExpressionUpdateOperation expressionUpdateOperation,
                TransactionOperation transactionOperation) {
        this.client = client;
        this.metadataRegistry = metadataRegistry;
        this.converterRegistry = converterRegistry;
        this.autoCreateTable = autoCreateTable;
        this.saveOperation = saveOperation;
        this.getOperation = getOperation;
        this.updateOperation = updateOperation;
        this.deleteOperation = deleteOperation;
        this.batchOperation = batchOperation;
        this.queryOperation = queryOperation;
        this.scanOperation = scanOperation;
        this.expressionUpdateOperation = expressionUpdateOperation;
        this.transactionOperation = transactionOperation;
    }

    public static DDM create(DynamoDbClient client) {
        if (client == null) throw new IllegalArgumentException("DynamoDbClient must not be null");
        return builder(client).build();
    }

    public static Builder builder(DynamoDbClient client) {
        if (client == null) throw new IllegalArgumentException("DynamoDbClient must not be null");
        return new Builder(client);
    }

    public DDM register(Class<?>... entityClasses) {
        for (Class<?> c : entityClasses) {
            metadataRegistry.register(c);
            log.debug("Registered entity class: {}", c.getName());
        }
        return this;
    }

    DynamoDbClient getClient() { return client; }
    MetadataRegistry getMetadataRegistry() { return metadataRegistry; }
    ConverterRegistry getConverterRegistry() { return converterRegistry; }
    boolean isAutoCreateTable() { return autoCreateTable; }

    /**
     * Returns the underlying DynamoDbClient for advanced use cases
     * that are not covered by the DDM API.
     *
     * @return the DynamoDbClient instance
     */
    public DynamoDbClient client() { return client; }

    // ======== Save (新增) ========

    /** Save a single entity. */
    public <T> void save(T entity) {
        saveOperation.save(entity);
    }

    /**
     * Save a single entity with a condition expression.
     *
     * <pre>{@code
     * // Insert only if item doesn't exist (prevent overwrite)
     * ddm.save(user, ConditionExpression.of("attribute_not_exists(userId)"));
     * }</pre>
     *
     * @param entity    the entity to save
     * @param condition the condition that must be met for the save to succeed
     * @throws com.jojo.framework.easydynamodb.exception.DynamoConditionFailedException if condition fails
     */
    public <T> void save(T entity, ConditionExpression condition) {
        saveOperation.save(entity, condition);
    }

    /** Batch save entities (auto-splits into batches of 25, parallel execution). */
    public <T> void saveBatch(List<T> entities) {
        batchOperation.saveBatch(entities);
    }

    // ======== Get (查询) ========

    /** Get a single entity by partition key. */
    public <T> T get(Class<T> clazz, Object partitionKey) {
        return getOperation.get(clazz, partitionKey);
    }

    /** Get a single entity by partition key + sort key. */
    public <T> T get(Class<T> clazz, Object pk, Object sk) {
        return getOperation.get(clazz, pk, sk);
    }

    /**
     * Get a single entity by partition key with consistent read option.
     * <p>
     * <b>Note:</b> If your partition key is of type {@code Object} and could be confused
     * with a sort key, use {@link #get(Class, Object, Object, boolean)} with an explicit
     * {@code null} sort key instead: {@code ddm.get(Foo.class, pk, null, true)}.
     *
     * @param clazz          the entity class
     * @param partitionKey   the partition key value
     * @param consistentRead true for strongly consistent read
     * @return the entity, or null if not found
     */
    public <T> T get(Class<T> clazz, Object partitionKey, boolean consistentRead) {
        return getOperation.get(clazz, partitionKey, null, consistentRead);
    }

    /**
     * Get a single entity by partition key + sort key with consistent read option.
     *
     * @param clazz          the entity class
     * @param pk             the partition key value
     * @param sk             the sort key value
     * @param consistentRead true for strongly consistent read
     * @return the entity, or null if not found
     */
    public <T> T get(Class<T> clazz, Object pk, Object sk, boolean consistentRead) {
        return getOperation.get(clazz, pk, sk, consistentRead);
    }

    /**
     * Get a single entity with projection (only return specific attributes).
     * <p>
     * Reduces bandwidth and read capacity consumption by only fetching the
     * listed attributes. Unmapped fields in the returned entity will be null.
     *
     * <pre>{@code
     * User user = ddm.get(User.class, "user-001", null, false, "userId, nickName, level");
     * }</pre>
     *
     * @param clazz               the entity class
     * @param pk                  the partition key value
     * @param sk                  the sort key value (nullable)
     * @param consistentRead      true for strongly consistent read
     * @param projectionExpression the projection expression
     * @return the entity, or null if not found
     */
    public <T> T get(Class<T> clazz, Object pk, Object sk, boolean consistentRead, String projectionExpression) {
        return getOperation.get(clazz, pk, sk, consistentRead, projectionExpression);
    }

    /** Batch get entities by exact keys (auto-splits into batches of 100, parallel execution). */
    public <T> List<T> getBatch(Class<T> clazz, List<KeyPair> keys) {
        return batchOperation.getBatch(clazz, keys);
    }

    // ======== Query (条件查询) ========

    public <T> QueryOperation.QueryBuilder<T> query(Class<T> clazz) {
        return queryOperation.query(clazz);
    }

    public <T> ScanOperation.ScanBuilder<T> scan(Class<T> clazz) {
        return scanOperation.scan(clazz);
    }

    // ======== Update (更新) ========

    /** Partial update: only update fields touched by the mutator. */
    public <T> void update(T entity, Consumer<T> mutator) {
        updateOperation.update(entity, mutator);
    }

    /**
     * Partial update with a condition expression.
     *
     * <pre>{@code
     * ddm.update(user, u -> u.setCoins(newCoins),
     *     ConditionExpression.builder()
     *         .expression("#coins = :expected")
     *         .name("#coins", "coins")
     *         .value(":expected", oldCoins)
     *         .build());
     * }</pre>
     *
     * @throws com.jojo.framework.easydynamodb.exception.DynamoConditionFailedException if condition fails
     */
    public <T> void update(T entity, Consumer<T> mutator, ConditionExpression condition) {
        updateOperation.update(entity, mutator, condition);
    }

    /** Full update: SET all non-null fields, REMOVE all null fields. */
    public <T> void updateAll(T entity) {
        updateOperation.updateAll(entity);
    }

    /** Full update with a condition expression. */
    public <T> void updateAll(T entity, ConditionExpression condition) {
        updateOperation.updateAll(entity, condition);
    }

    /** Batch partial update: apply the same mutator to each entity in parallel. */
    public <T> void updateBatch(List<T> entities, Consumer<T> mutator) {
        updateOperation.updateBatch(entities, mutator);
    }

    /** Batch full update: updateAll for each entity in parallel. */
    public <T> void updateAllBatch(List<T> entities) {
        updateOperation.updateAllBatch(entities);
    }

    // ======== Expression Update (表达式更新 — 原子操作) ========

    /**
     * Creates an expression-based update builder for atomic operations.
     * <p>
     * Use this for atomic increments/decrements, raw SET/REMOVE/ADD/DELETE
     * expressions, and conditional updates that go beyond the entity-level
     * diff-based update.
     *
     * <pre>{@code
     * // Atomic increment with condition
     * ddm.expressionUpdate(User.class, "user-001")
     *     .increment("coins", 100)
     *     .condition("#coins >= :min", c -> c.name("#coins", "coins").value(":min", 0))
     *     .execute();
     *
     * // Multiple atomic operations
     * ddm.expressionUpdate(User.class, "user-001")
     *     .increment("exp", 50)
     *     .set("lastLoginTime", Instant.now().toString())
     *     .remove("tempFlag")
     *     .execute();
     * }</pre>
     *
     * @param clazz        the entity class
     * @param partitionKey the partition key value
     * @return a fluent expression update builder
     */
    public <T> ExpressionUpdateOperation.ExpressionUpdateBuilder<T> expressionUpdate(Class<T> clazz, Object partitionKey) {
        return expressionUpdateOperation.expressionUpdate(clazz, partitionKey);
    }

    /**
     * Creates an expression-based update builder with composite key.
     *
     * @param clazz        the entity class
     * @param partitionKey the partition key value
     * @param sortKey      the sort key value
     * @return a fluent expression update builder
     */
    public <T> ExpressionUpdateOperation.ExpressionUpdateBuilder<T> expressionUpdate(Class<T> clazz, Object partitionKey, Object sortKey) {
        return expressionUpdateOperation.expressionUpdate(clazz, partitionKey, sortKey);
    }

    // ======== Delete (删除) ========

    /** Delete a single entity by partition key. */
    public <T> void delete(Class<T> clazz, Object partitionKey) {
        deleteOperation.delete(clazz, partitionKey);
    }

    /** Delete a single entity by partition key + sort key. */
    public <T> void delete(Class<T> clazz, Object pk, Object sk) {
        deleteOperation.delete(clazz, pk, sk);
    }

    /**
     * Delete a single entity with a condition expression.
     *
     * <pre>{@code
     * ddm.delete(User.class, "user-001", null,
     *     ConditionExpression.builder()
     *         .expression("#status = :expected")
     *         .name("#status", "status")
     *         .value(":expected", "INACTIVE")
     *         .build());
     * }</pre>
     *
     * @throws com.jojo.framework.easydynamodb.exception.DynamoConditionFailedException if condition fails
     */
    public <T> void delete(Class<T> clazz, Object pk, Object sk, ConditionExpression condition) {
        deleteOperation.delete(clazz, pk, sk, condition);
    }

    /** Batch delete by exact keys (auto-splits into batches of 25, parallel execution). */
    public <T> void deleteBatch(Class<T> clazz, List<KeyPair> keys) {
        batchOperation.deleteBatch(clazz, keys);
    }

    /**
     * Delete all items matching a condition. Returns the number of items deleted.
     */
    public <T> int deleteByCondition(Class<T> clazz,
                                     String filterExpression,
                                     Map<String, AttributeValue> expressionValues) {
        return deleteOperation.deleteByCondition(clazz, filterExpression, expressionValues, null);
    }

    /**
     * Delete all items matching a condition. Returns the number of items deleted.
     */
    public <T> int deleteByCondition(Class<T> clazz,
                                     String filterExpression,
                                     Map<String, AttributeValue> expressionValues,
                                     Map<String, String> expressionNames) {
        return deleteOperation.deleteByCondition(clazz, filterExpression, expressionValues, expressionNames);
    }

    /**
     * Delete all items matching a condition using auto-converted values.
     * Values are automatically converted to AttributeValue using the same rules
     * as {@code query().value()} — supports String, Number, Boolean, Enum, Instant, etc.
     *
     * <pre>{@code
     * int deleted = ddm.deleteByConditionWithValues(Game.class,
     *     "rating < :minRating",
     *     Map.of(":minRating", 5.0));
     * }</pre>
     *
     * @return the number of items deleted
     */
    public <T> int deleteByConditionWithValues(Class<T> clazz,
                                               String filterExpression,
                                               Map<String, Object> values) {
        return deleteByConditionWithValues(clazz, filterExpression, values, null);
    }

    /**
     * Delete all items matching a condition using auto-converted values and expression names.
     *
     * @return the number of items deleted
     */
    public <T> int deleteByConditionWithValues(Class<T> clazz,
                                               String filterExpression,
                                               Map<String, Object> values,
                                               Map<String, String> expressionNames) {
        Map<String, AttributeValue> converted = new HashMap<>();
        if (values != null) {
            values.forEach((k, v) -> converted.put(k, AttributeValues.of(v)));
        }
        return deleteOperation.deleteByCondition(clazz, filterExpression, converted, expressionNames);
    }

    // ======== Transaction (事务) ========

    /**
     * Creates a transaction write builder for atomic multi-item operations.
     * <p>
     * A transaction guarantees all-or-nothing: either all items succeed or
     * none are applied. Supports up to 100 items per transaction.
     *
     * <pre>{@code
     * ddm.transact()
     *     .put(newUser)
     *     .put(newWallet, ConditionExpression.of("attribute_not_exists(userId)"))
     *     .update(User.class, "user-001", u -> u
     *         .increment("coins", 100)
     *         .condition("#coins >= :min", c -> c.name("#coins", "coins").value(":min", 0)))
     *     .delete(TempRecord.class, "temp-001")
     *     .conditionCheck(Inventory.class, "user-001", "item-sword",
     *         ConditionExpression.builder()
     *             .expression("#count >= :required")
     *             .name("#count", "count")
     *             .value(":required", 1)
     *             .build())
     *     .execute();
     * }</pre>
     *
     * @return a fluent transaction write builder
     */
    public TransactionOperation.TransactWriteBuilder transact() {
        return transactionOperation.transact();
    }

    /**
     * Creates a transaction read builder for consistent multi-item reads.
     * <p>
     * Guarantees a consistent snapshot across all items at the time of the read.
     *
     * <pre>{@code
     * var result = ddm.transactGet()
     *     .get(User.class, "user-001")
     *     .get(Wallet.class, "user-001")
     *     .execute();
     *
     * User user = result.get(0, User.class);
     * Wallet wallet = result.get(1, Wallet.class);
     * }</pre>
     *
     * @return a fluent transaction read builder
     */
    public TransactionOperation.TransactGetBuilder transactGet() {
        return transactionOperation.transactGet();
    }

    // ======== Builder ========

    public static class Builder {
        private final DynamoDbClient client;
        private String tablePrefix = "";
        private boolean autoCreateTable = false;
        private final ConverterRegistry converterRegistry = new ConverterRegistry();
        private TableNameResolver tableNameResolver;
        private Executor batchExecutor;
        private Class<?>[] pendingEntityClasses;
        private boolean enableLogging = false;
        private Level logLevel = Level.INFO;

        private Builder(DynamoDbClient client) { this.client = client; }

        public Builder tablePrefix(String prefix) {
            this.tablePrefix = prefix == null ? "" : prefix;
            return this;
        }

        public Builder autoCreateTable(boolean enabled) {
            this.autoCreateTable = enabled;
            return this;
        }

        public Builder tableNameResolver(TableNameResolver resolver) {
            this.tableNameResolver = resolver;
            return this;
        }

        public Builder batchExecutor(Executor executor) {
            this.batchExecutor = executor;
            return this;
        }

        public Builder registerConverter(Class<?> type, AttributeConverter<?> converter) {
            converterRegistry.register(type, converter);
            return this;
        }

        public Builder register(Class<?>... entityClasses) {
            this.pendingEntityClasses = entityClasses;
            return this;
        }

        /**
         * Enable or disable logging for all EasyDynamodb operations.
         * Disabled by default. When enabled, uses SLF4J with the configured log level.
         */
        public Builder enableLogging(boolean enabled) {
            this.enableLogging = enabled;
            return this;
        }

        /**
         * Set the minimum log level. Default is INFO.
         * Only effective when logging is enabled.
         *
         * @param level one of {@link Level#TRACE}, {@link Level#DEBUG},
         *              {@link Level#INFO}, {@link Level#WARN}, {@link Level#ERROR}
         */
        public Builder logLevel(Level level) {
            this.logLevel = level;
            return this;
        }

        public DDM build() {
            // Configure global logging state before anything else
            DdmLogger.configure(enableLogging, logLevel);

            DdmLogger buildLog = DdmLogger.getLogger(DDM.class);
            buildLog.info("Building DDM instance [autoCreateTable={}, tablePrefix='{}', logging={}({})]",
                    autoCreateTable, tablePrefix, enableLogging, logLevel);

            MetadataRegistry mr = new MetadataRegistry(converterRegistry, tablePrefix, tableNameResolver);
            if (pendingEntityClasses != null) {
                for (Class<?> c : pendingEntityClasses) {
                    mr.register(c);
                    buildLog.debug("Pre-registered entity class: {}", c.getName());
                }
            }

            TableCreateOperation tco = new TableCreateOperation(client);
            SaveOperation save = new SaveOperation(client, mr, autoCreateTable, tco);
            GetOperation get = new GetOperation(client, mr);
            UpdateOperation update = batchExecutor != null
                    ? new UpdateOperation(client, mr, batchExecutor)
                    : new UpdateOperation(client, mr);
            DeleteOperation delete = new DeleteOperation(client, mr);
            BatchOperation batch = batchExecutor != null
                    ? new BatchOperation(client, mr, save, get, batchExecutor)
                    : new BatchOperation(client, mr, save, get);
            QueryOperation query = new QueryOperation(client, mr, get);
            ScanOperation scan = new ScanOperation(client, mr, get);
            ExpressionUpdateOperation exprUpdate = new ExpressionUpdateOperation(client, mr);
            TransactionOperation transaction = new TransactionOperation(client, mr, save);

            buildLog.info("DDM instance built successfully");

            return new DDM(client, mr, converterRegistry, autoCreateTable,
                    save, get, update, delete, batch, query, scan,
                    exprUpdate, transaction);
        }
    }
}
