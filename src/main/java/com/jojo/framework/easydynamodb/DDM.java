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
        log.debug("Saving entity of type: {}", entity.getClass().getSimpleName());
        saveOperation.save(entity);
        log.info("Saved entity of type: {}", entity.getClass().getSimpleName());
    }

    /** Batch save entities (auto-splits into batches of 25, parallel execution). */
    public <T> void saveBatch(List<T> entities) {
        log.debug("Batch saving {} entities", entities.size());
        batchOperation.saveBatch(entities);
        log.info("Batch saved {} entities", entities.size());
    }

    // ======== Get (查询) ========

    /** Get a single entity by partition key. */
    public <T> T get(Class<T> clazz, Object partitionKey) {
        log.debug("Getting {} by pk={}", clazz.getSimpleName(), partitionKey);
        T result = getOperation.get(clazz, partitionKey);
        log.info("Get {} by pk={} -> {}", clazz.getSimpleName(), partitionKey, result != null ? "found" : "not found");
        return result;
    }

    /** Get a single entity by partition key + sort key. */
    public <T> T get(Class<T> clazz, Object pk, Object sk) {
        log.debug("Getting {} by pk={}, sk={}", clazz.getSimpleName(), pk, sk);
        T result = getOperation.get(clazz, pk, sk);
        log.info("Get {} by pk={}, sk={} -> {}", clazz.getSimpleName(), pk, sk, result != null ? "found" : "not found");
        return result;
    }

    /** Batch get entities by exact keys (auto-splits into batches of 100, parallel execution). */
    public <T> List<T> getBatch(Class<T> clazz, List<KeyPair> keys) {
        log.debug("Batch getting {} keys for {}", keys.size(), clazz.getSimpleName());
        List<T> results = batchOperation.getBatch(clazz, keys);
        log.info("Batch get {} -> returned {} items", clazz.getSimpleName(), results.size());
        return results;
    }

    // ======== Query (条件查询) ========

    public <T> QueryOperation.QueryBuilder<T> query(Class<T> clazz) {
        log.debug("Creating query builder for {}", clazz.getSimpleName());
        return queryOperation.query(clazz);
    }

    public <T> ScanOperation.ScanBuilder<T> scan(Class<T> clazz) {
        log.debug("Creating scan builder for {}", clazz.getSimpleName());
        return scanOperation.scan(clazz);
    }

    // ======== Update (更新) ========

    /** Partial update: only update fields touched by the mutator. */
    public <T> void update(T entity, Consumer<T> mutator) {
        log.debug("Partial updating entity of type: {}", entity.getClass().getSimpleName());
        updateOperation.update(entity, mutator);
        log.info("Partial updated entity of type: {}", entity.getClass().getSimpleName());
    }

    /** Full update: SET all non-null fields, REMOVE all null fields. */
    public <T> void updateAll(T entity) {
        log.debug("Full updating entity of type: {}", entity.getClass().getSimpleName());
        updateOperation.updateAll(entity);
        log.info("Full updated entity of type: {}", entity.getClass().getSimpleName());
    }

    /** Batch partial update: apply the same mutator to each entity in parallel. */
    public <T> void updateBatch(List<T> entities, Consumer<T> mutator) {
        log.debug("Batch partial updating {} entities", entities.size());
        updateOperation.updateBatch(entities, mutator);
        log.info("Batch partial updated {} entities", entities.size());
    }

    /** Batch full update: updateAll for each entity in parallel. */
    public <T> void updateAllBatch(List<T> entities) {
        log.debug("Batch full updating {} entities", entities.size());
        updateOperation.updateAllBatch(entities);
        log.info("Batch full updated {} entities", entities.size());
    }

    // ======== Delete (删除) ========

    /** Delete a single entity by partition key. */
    public <T> void delete(Class<T> clazz, Object partitionKey) {
        log.debug("Deleting {} by pk={}", clazz.getSimpleName(), partitionKey);
        deleteOperation.delete(clazz, partitionKey);
        log.info("Deleted {} by pk={}", clazz.getSimpleName(), partitionKey);
    }

    /** Delete a single entity by partition key + sort key. */
    public <T> void delete(Class<T> clazz, Object pk, Object sk) {
        log.debug("Deleting {} by pk={}, sk={}", clazz.getSimpleName(), pk, sk);
        deleteOperation.delete(clazz, pk, sk);
        log.info("Deleted {} by pk={}, sk={}", clazz.getSimpleName(), pk, sk);
    }

    /** Batch delete by exact keys (auto-splits into batches of 25, parallel execution). */
    public <T> void deleteBatch(Class<T> clazz, List<KeyPair> keys) {
        log.debug("Batch deleting {} keys for {}", keys.size(), clazz.getSimpleName());
        batchOperation.deleteBatch(clazz, keys);
        log.info("Batch deleted {} keys for {}", keys.size(), clazz.getSimpleName());
    }

    /**
     * Delete all items matching a condition. Returns the number of items deleted.
     */
    public <T> int deleteByCondition(Class<T> clazz,
                                     String filterExpression,
                                     Map<String, AttributeValue> expressionValues,
                                     Map<String, String> expressionNames) {
        log.debug("Deleting {} by condition: {}", clazz.getSimpleName(), filterExpression);
        int count = deleteOperation.deleteByCondition(clazz, filterExpression, expressionValues, expressionNames);
        log.info("Deleted {} items of {} by condition", count, clazz.getSimpleName());
        return count;
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
            UpdateOperation update = new UpdateOperation(client, mr);
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
