package com.jojo.framework.easydynamodb;

import com.jojo.framework.easydynamodb.converter.AttributeConverter;
import com.jojo.framework.easydynamodb.converter.ConverterRegistry;
import com.jojo.framework.easydynamodb.logging.DdmLogger;
import com.jojo.framework.easydynamodb.metadata.MetadataRegistry;
import com.jojo.framework.easydynamodb.metadata.TableNameResolver;
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

    private DDM(DynamoDbClient client, MetadataRegistry metadataRegistry,
                ConverterRegistry converterRegistry, boolean autoCreateTable,
                SaveOperation saveOperation, GetOperation getOperation,
                UpdateOperation updateOperation, DeleteOperation deleteOperation,
                BatchOperation batchOperation, QueryOperation queryOperation,
                ScanOperation scanOperation) {
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

    // ======== Save (新增) ========

    /** Save a single entity. */
    public <T> void save(T entity) {
        saveOperation.save(entity);
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

    /** Full update: SET all non-null fields, REMOVE all null fields. */
    public <T> void updateAll(T entity) {
        updateOperation.updateAll(entity);
    }

    /** Batch partial update: apply the same mutator to each entity in parallel. */
    public <T> void updateBatch(List<T> entities, Consumer<T> mutator) {
        updateOperation.updateBatch(entities, mutator);
    }

    /** Batch full update: updateAll for each entity in parallel. */
    public <T> void updateAllBatch(List<T> entities) {
        updateOperation.updateAllBatch(entities);
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

            buildLog.info("DDM instance built successfully");

            return new DDM(client, mr, converterRegistry, autoCreateTable,
                    save, get, update, delete, batch, query, scan);
        }
    }
}
