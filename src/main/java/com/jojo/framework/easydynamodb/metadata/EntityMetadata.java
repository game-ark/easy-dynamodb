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
 * 单个实体类的缓存元数据。持有表名、键字段、所有映射字段以及用于无参构造的快速
 * {@link MethodHandle}。所有反射工作在注册时一次性完成，运行时操作零反射开销。
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

    /**
     * Constructs EntityMetadata without GSI information (defaults to empty list).
     * 构造不含 GSI 信息的 EntityMetadata（默认为空列表）。
     *
     * @param entityClass          the entity class / 实体类
     * @param tableName            the DynamoDB table name / DynamoDB 表名
     * @param partitionKey         metadata for the partition key field / 分区键字段的元数据
     * @param sortKey              metadata for the sort key field, may be null / 排序键字段的元数据，可为 null
     * @param fields               list of all mapped field metadata / 所有映射字段元数据的列表
     * @param fieldByAttributeName map from DynamoDB attribute name to field metadata / DynamoDB 属性名到字段元数据的映射
     */
    public EntityMetadata(Class<?> entityClass,
                          String tableName,
                          FieldMetadata partitionKey,
                          FieldMetadata sortKey,
                          List<FieldMetadata> fields,
                          Map<String, FieldMetadata> fieldByAttributeName) {
        this(entityClass, tableName, partitionKey, sortKey, fields, fieldByAttributeName, Collections.emptyList());
    }

    /**
     * Constructs EntityMetadata with full GSI information.
     * 构造包含完整 GSI 信息的 EntityMetadata。
     *
     * @param entityClass            the entity class / 实体类
     * @param tableName              the DynamoDB table name / DynamoDB 表名
     * @param partitionKey           metadata for the partition key field / 分区键字段的元数据
     * @param sortKey                metadata for the sort key field, may be null / 排序键字段的元数据，可为 null
     * @param fields                 list of all mapped field metadata / 所有映射字段元数据的列表
     * @param fieldByAttributeName   map from DynamoDB attribute name to field metadata / DynamoDB 属性名到字段元数据的映射
     * @param globalSecondaryIndexes list of GSI metadata / 全局二级索引元数据列表
     */
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
     * 使用缓存的 {@link MethodHandle} 创建实体类的新实例。
     * 在 JIT 预热后，这比 {@code Class.newInstance()} 或反射构造调用快得多。
     *
     * @return a new entity instance / 新的实体实例
     * @throws DynamoConfigException if instantiation fails / 实例化失败时抛出
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

    /**
     * Returns the entity class.
     * 返回实体类。
     *
     * @return the entity class / 实体类
     */
    public Class<?> getEntityClass() {
        return entityClass;
    }

    /**
     * Returns the resolved DynamoDB table name.
     * 返回解析后的 DynamoDB 表名。
     *
     * @return the table name / 表名
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Returns the partition key field metadata.
     * 返回分区键字段的元数据。
     *
     * @return the partition key metadata / 分区键元数据
     */
    public FieldMetadata getPartitionKey() {
        return partitionKey;
    }

    /**
     * Returns the sort key field metadata, or null if no sort key is defined.
     * 返回排序键字段的元数据，如果未定义排序键则返回 null。
     *
     * @return the sort key metadata, or null / 排序键元数据，或 null
     */
    public FieldMetadata getSortKey() {
        return sortKey;
    }

    /**
     * Returns an unmodifiable list of all mapped field metadata.
     * 返回所有映射字段元数据的不可变列表。
     *
     * @return the list of field metadata / 字段元数据列表
     */
    public List<FieldMetadata> getFields() {
        return fields;
    }

    /**
     * Returns an unmodifiable map from DynamoDB attribute name to field metadata.
     * 返回从 DynamoDB 属性名到字段元数据的不可变映射。
     *
     * @return the attribute-name-to-field-metadata map / 属性名到字段元数据的映射
     */
    public Map<String, FieldMetadata> getFieldByAttributeName() {
        return fieldByAttributeName;
    }

    /**
     * Returns an unmodifiable list of global secondary index metadata.
     * 返回全局二级索引元数据的不可变列表。
     *
     * @return the list of GSI metadata / GSI 元数据列表
     */
    public List<GsiMetadata> getGlobalSecondaryIndexes() {
        return globalSecondaryIndexes;
    }
}
