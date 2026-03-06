package com.jojo.framework.easydynamodb;

import com.jojo.framework.easydynamodb.converter.AttributeConverter;
import com.jojo.framework.easydynamodb.converter.ConverterRegistry;
import com.jojo.framework.easydynamodb.metadata.MetadataRegistry;
import com.jojo.framework.easydynamodb.metadata.TableNameResolver;
import com.jojo.framework.easydynamodb.model.KeyPair;
import com.jojo.framework.easydynamodb.operation.BatchOperation;
import com.jojo.framework.easydynamodb.operation.GetOperation;
import com.jojo.framework.easydynamodb.operation.SaveOperation;
import com.jojo.framework.easydynamodb.operation.TableCreateOperation;
import com.jojo.framework.easydynamodb.operation.UpdateOperation;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.List;
import java.util.function.Consumer;

/**
 * DynamodbDataManager — the core entry point for all DynamoDB operations.
 * <p>
 * Use {@link #create(DynamoDbClient)} for quick setup with defaults, or
 * {@link #builder(DynamoDbClient)} for fine-grained configuration.
 */
public class DDM {

    private final DynamoDbClient client;
    private final MetadataRegistry metadataRegistry;
    private final ConverterRegistry converterRegistry;
    private final boolean autoCreateTable;
    private final SaveOperation saveOperation;
    private final GetOperation getOperation;
    private final UpdateOperation updateOperation;
    private final BatchOperation batchOperation;

    private DDM(DynamoDbClient client,
                MetadataRegistry metadataRegistry,
                ConverterRegistry converterRegistry,
                boolean autoCreateTable,
                SaveOperation saveOperation,
                GetOperation getOperation,
                UpdateOperation updateOperation,
                BatchOperation batchOperation) {
        this.client = client;
        this.metadataRegistry = metadataRegistry;
        this.converterRegistry = converterRegistry;
        this.autoCreateTable = autoCreateTable;
        this.saveOperation = saveOperation;
        this.getOperation = getOperation;
        this.updateOperation = updateOperation;
        this.batchOperation = batchOperation;
    }

    // ---- Static factory methods ----

    public static DDM create(DynamoDbClient client) {
        if (client == null) {
            throw new IllegalArgumentException("DynamoDbClient must not be null");
        }
        return builder(client).build();
    }

    public static Builder builder(DynamoDbClient client) {
        if (client == null) {
            throw new IllegalArgumentException("DynamoDbClient must not be null");
        }
        return new Builder(client);
    }

    // ---- Entity registration ----

    public DDM register(Class<?>... entityClasses) {
        for (Class<?> entityClass : entityClasses) {
            metadataRegistry.register(entityClass);
        }
        return this;
    }

    // ---- Accessors (package-private, for Operation classes) ----

    DynamoDbClient getClient() {
        return client;
    }

    MetadataRegistry getMetadataRegistry() {
        return metadataRegistry;
    }

    ConverterRegistry getConverterRegistry() {
        return converterRegistry;
    }

    boolean isAutoCreateTable() {
        return autoCreateTable;
    }

    // ---- CRUD operations ----

    public <T> void save(T entity) {
        saveOperation.save(entity);
    }

    public <T> void saveBatch(List<T> entities) {
        batchOperation.saveBatch(entities);
    }

    public <T> T get(Class<T> clazz, Object partitionKey) {
        return getOperation.get(clazz, partitionKey);
    }

    public <T> T get(Class<T> clazz, Object partitionKey, Object sortKey) {
        return getOperation.get(clazz, partitionKey, sortKey);
    }

    public <T> List<T> getBatch(Class<T> clazz, List<KeyPair> keys) {
        return batchOperation.getBatch(clazz, keys);
    }

    public <T> void update(T entity, Consumer<T> mutator) {
        updateOperation.update(entity, mutator);
    }

    public <T> void updateAll(T entity) {
        updateOperation.updateAll(entity);
    }

    // ---- Builder ----

    public static class Builder {

        private final DynamoDbClient client;
        private String tablePrefix = "";
        private boolean autoCreateTable = false;
        private final ConverterRegistry converterRegistry = new ConverterRegistry();
        private TableNameResolver tableNameResolver;
        private Class<?>[] pendingEntityClasses;

        private Builder(DynamoDbClient client) {
            this.client = client;
        }

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

        public Builder registerConverter(Class<?> type, AttributeConverter<?> converter) {
            converterRegistry.register(type, converter);
            return this;
        }

        public Builder register(Class<?>... entityClasses) {
            this.pendingEntityClasses = entityClasses;
            return this;
        }

        public DDM build() {
            MetadataRegistry metadataRegistry = new MetadataRegistry(converterRegistry, tablePrefix, tableNameResolver);

            if (pendingEntityClasses != null) {
                for (Class<?> entityClass : pendingEntityClasses) {
                    metadataRegistry.register(entityClass);
                }
            }

            TableCreateOperation tableCreateOperation = new TableCreateOperation(client);
            SaveOperation saveOperation = new SaveOperation(client, metadataRegistry, autoCreateTable, tableCreateOperation);
            GetOperation getOperation = new GetOperation(client, metadataRegistry);
            UpdateOperation updateOperation = new UpdateOperation(client, metadataRegistry);
            BatchOperation batchOperation = new BatchOperation(client, metadataRegistry, saveOperation, getOperation);

            return new DDM(client, metadataRegistry, converterRegistry, autoCreateTable,
                    saveOperation, getOperation, updateOperation, batchOperation);
        }
    }
}
