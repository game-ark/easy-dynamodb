package com.jojo.framework.easydynamodb.metadata;

import com.jojo.framework.easydynamodb.exception.DynamoConfigException;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Cached metadata for a single entity class. Holds the table name, key fields,
 * all mapped fields, and a fast {@link MethodHandle} for no-arg construction.
 * All reflection work is performed once at registration time so that runtime
 * operations incur zero reflection overhead.
 */
public class EntityMetadata {

    private final Class<?> entityClass;
    private final String tableName;
    private final FieldMetadata partitionKey;
    private final FieldMetadata sortKey;          // nullable
    private final List<FieldMetadata> fields;
    private final Map<String, FieldMetadata> fieldByAttributeName;
    private final List<GsiMetadata> globalSecondaryIndexes;
    private final MethodHandle constructor;

    public EntityMetadata(Class<?> entityClass,
                          String tableName,
                          FieldMetadata partitionKey,
                          FieldMetadata sortKey,
                          List<FieldMetadata> fields,
                          Map<String, FieldMetadata> fieldByAttributeName) {
        this(entityClass, tableName, partitionKey, sortKey, fields, fieldByAttributeName, Collections.emptyList());
    }

    public EntityMetadata(Class<?> entityClass,
                          String tableName,
                          FieldMetadata partitionKey,
                          FieldMetadata sortKey,
                          List<FieldMetadata> fields,
                          Map<String, FieldMetadata> fieldByAttributeName,
                          List<GsiMetadata> globalSecondaryIndexes) {
        this.entityClass = entityClass;
        this.tableName = tableName;
        this.partitionKey = partitionKey;
        this.sortKey = sortKey;
        this.fields = Collections.unmodifiableList(fields);
        this.fieldByAttributeName = Collections.unmodifiableMap(fieldByAttributeName);
        this.globalSecondaryIndexes = Collections.unmodifiableList(globalSecondaryIndexes);

        MethodHandle ctor;
        try {
            MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(entityClass, MethodHandles.lookup());
            ctor = lookup.findConstructor(entityClass, MethodType.methodType(void.class));
        } catch (IllegalAccessException e) {
            // privateLookupIn failed (module restriction) — fallback to caller's lookup
            try {
                ctor = MethodHandles.lookup()
                        .findConstructor(entityClass, MethodType.methodType(void.class));
            } catch (NoSuchMethodException | IllegalAccessException ex) {
                throw new DynamoConfigException(
                        "Cannot access no-arg constructor of " + entityClass.getName()
                                + ". Ensure the constructor is public.");
            }
        } catch (NoSuchMethodException e) {
            throw new DynamoConfigException(
                    "Entity class " + entityClass.getName() + " must have a public no-arg constructor");
        }
        this.constructor = ctor;
    }

    /**
     * Creates a new instance of the entity class using the cached {@link MethodHandle}.
     * This is significantly faster than {@code Class.newInstance()} or reflective
     * constructor invocation after JIT warm-up.
     */
    public Object newInstance() {
        try {
            return constructor.invoke();
        } catch (Throwable e) {
            throw new DynamoConfigException(
                    "Failed to create instance of " + entityClass.getName() + ": " + e.getMessage());
        }
    }

    // ---- Getters ----

    public Class<?> getEntityClass() {
        return entityClass;
    }

    public String getTableName() {
        return tableName;
    }

    public FieldMetadata getPartitionKey() {
        return partitionKey;
    }

    public FieldMetadata getSortKey() {
        return sortKey;
    }

    public List<FieldMetadata> getFields() {
        return fields;
    }

    public Map<String, FieldMetadata> getFieldByAttributeName() {
        return fieldByAttributeName;
    }

    public List<GsiMetadata> getGlobalSecondaryIndexes() {
        return globalSecondaryIndexes;
    }
}
