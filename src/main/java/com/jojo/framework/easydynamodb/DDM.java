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
 * DynamoDataManager (DDM) — the core entry point for all DynamoDB operations.
 * DynamoDataManager（DDM）— 所有 DynamoDB 操作的核心入口类。
 * <p>
 * Use {@link #create(DynamoDbClient)} for quick setup with defaults, or
 * {@link #builder(DynamoDbClient)} for fine-grained configuration.
 * <p>
 * 使用 {@link #create(DynamoDbClient)} 快速创建默认实例，或使用
 * {@link #builder(DynamoDbClient)} 进行精细化配置。
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

    /**
     * Creates a DDM instance with default settings.
     * 使用默认配置创建 DDM 实例。
     *
     * @param client the AWS DynamoDB client / AWS DynamoDB 客户端
     * @return a new DDM instance / 新的 DDM 实例
     * @throws IllegalArgumentException if client is null / 如果 client 为 null
     */
    public static DDM create(DynamoDbClient client) {
        if (client == null) throw new IllegalArgumentException("DynamoDbClient must not be null");
        return builder(client).build();
    }

    /**
     * Creates a Builder for fine-grained DDM configuration.
     * 创建 Builder 以进行精细化 DDM 配置。
     *
     * @param client the AWS DynamoDB client / AWS DynamoDB 客户端
     * @return a new Builder instance / 新的 Builder 实例
     * @throws IllegalArgumentException if client is null / 如果 client 为 null
     */
    public static Builder builder(DynamoDbClient client) {
        if (client == null) throw new IllegalArgumentException("DynamoDbClient must not be null");
        return new Builder(client);
    }

    /**
     * Registers one or more entity classes at runtime. Auto-registration also happens on first use.
     * 在运行时注册一个或多个实体类。首次使用时也会自动注册。
     *
     * @param entityClasses the entity classes to register / 要注册的实体类
     * @return this DDM instance for chaining / 当前 DDM 实例（支持链式调用）
     */
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
     * Returns the underlying DynamoDbClient for advanced use cases not covered by the DDM API.
     * 返回底层 DynamoDbClient，用于 DDM API 未覆盖的高级场景。
     *
     * @return the DynamoDbClient instance / DynamoDbClient 实例
     */
    public DynamoDbClient client() { return client; }

    // ======== Save (保存) ========

    /**
     * Saves a single entity to DynamoDB (upsert semantics — overwrites if key exists).
     * 保存单个实体到 DynamoDB（upsert 语义 — 如果主键已存在则覆盖）。
     *
     * @param entity the entity to save / 要保存的实体
     * @param <T>    the entity type / 实体类型
     */
    public <T> void save(T entity) {
        saveOperation.save(entity);
    }

    /**
     * Saves a single entity with a condition expression.
     * 保存单个实体，附带条件表达式。
     *
     * <pre>{@code
     * // Insert only if item doesn't exist (prevent overwrite)
     * // 仅在不存在时插入（防止覆盖）
     * ddm.save(user, ConditionExpression.of("attribute_not_exists(userId)"));
     * }</pre>
     *
     * @param entity    the entity to save / 要保存的实体
     * @param condition the condition that must be met for the save to succeed / 保存成功必须满足的条件
     * @param <T>       the entity type / 实体类型
     * @throws com.jojo.framework.easydynamodb.exception.DynamoConditionFailedException if condition fails / 条件不满足时抛出
     */
    public <T> void save(T entity, ConditionExpression condition) {
        saveOperation.save(entity, condition);
    }

    /**
     * Batch saves entities (auto-splits into batches of 25, parallel execution with retry).
     * 批量保存实体（自动按 25 条分批，并行执行，带重试机制）。
     *
     * @param entities the list of entities to save / 要保存的实体列表
     * @param <T>      the entity type / 实体类型
     * @throws com.jojo.framework.easydynamodb.exception.DynamoBatchException if partial failures remain after retries / 重试后仍有部分失败时抛出
     */
    public <T> void saveBatch(List<T> entities) {
        batchOperation.saveBatch(entities);
    }

    // ======== Get (查询/获取) ========

    /**
     * Gets a single entity by partition key.
     * 通过分区键获取单个实体。
     *
     * @param clazz        the entity class / 实体类
     * @param partitionKey the partition key value / 分区键值
     * @param <T>          the entity type / 实体类型
     * @return the entity, or null if not found / 实体对象，未找到时返回 null
     */
    public <T> T get(Class<T> clazz, Object partitionKey) {
        return getOperation.get(clazz, partitionKey);
    }

    /**
     * Gets a single entity by partition key + sort key.
     * 通过分区键 + 排序键获取单个实体。
     *
     * @param clazz the entity class / 实体类
     * @param pk    the partition key value / 分区键值
     * @param sk    the sort key value / 排序键值
     * @param <T>   the entity type / 实体类型
     * @return the entity, or null if not found / 实体对象，未找到时返回 null
     */
    public <T> T get(Class<T> clazz, Object pk, Object sk) {
        return getOperation.get(clazz, pk, sk);
    }

    /**
     * Gets a single entity by partition key with consistent read option.
     * 通过分区键获取单个实体，支持强一致性读取。
     * <p>
     * <b>Note / 注意:</b> If your partition key is of type {@code Object} and could be confused
     * with a sort key, use {@link #get(Class, Object, Object, boolean)} with an explicit
     * {@code null} sort key instead: {@code ddm.get(Foo.class, pk, null, true)}.
     * <br>
     * 如果分区键类型为 {@code Object} 可能与排序键混淆，请使用
     * {@link #get(Class, Object, Object, boolean)} 并显式传入 {@code null} 排序键。
     *
     * @param clazz          the entity class / 实体类
     * @param partitionKey   the partition key value / 分区键值
     * @param consistentRead true for strongly consistent read / true 表示强一致性读取
     * @param <T>            the entity type / 实体类型
     * @return the entity, or null if not found / 实体对象，未找到时返回 null
     */
    public <T> T get(Class<T> clazz, Object partitionKey, boolean consistentRead) {
        return getOperation.get(clazz, partitionKey, null, consistentRead);
    }

    /**
     * Gets a single entity by partition key + sort key with consistent read option.
     * 通过分区键 + 排序键获取单个实体，支持强一致性读取。
     *
     * @param clazz          the entity class / 实体类
     * @param pk             the partition key value / 分区键值
     * @param sk             the sort key value / 排序键值
     * @param consistentRead true for strongly consistent read / true 表示强一致性读取
     * @param <T>            the entity type / 实体类型
     * @return the entity, or null if not found / 实体对象，未找到时返回 null
     */
    public <T> T get(Class<T> clazz, Object pk, Object sk, boolean consistentRead) {
        return getOperation.get(clazz, pk, sk, consistentRead);
    }

    /**
     * Gets a single entity with projection (only return specific attributes).
     * 获取单个实体，支持投影查询（仅返回指定属性）。
     * <p>
     * Reduces bandwidth and read capacity consumption by only fetching the listed attributes.
     * Unmapped fields in the returned entity will be null.
     * <br>
     * 通过仅获取指定属性来减少带宽和读取容量消耗。未映射的字段将为 null。
     *
     * <pre>{@code
     * User user = ddm.get(User.class, "user-001", null, false, "userId, nickName, level");
     * }</pre>
     *
     * @param clazz               the entity class / 实体类
     * @param pk                  the partition key value / 分区键值
     * @param sk                  the sort key value (nullable) / 排序键值（可为 null）
     * @param consistentRead      true for strongly consistent read / true 表示强一致性读取
     * @param projectionExpression the projection expression / 投影表达式
     * @param <T>                 the entity type / 实体类型
     * @return the entity, or null if not found / 实体对象，未找到时返回 null
     */
    public <T> T get(Class<T> clazz, Object pk, Object sk, boolean consistentRead, String projectionExpression) {
        return getOperation.get(clazz, pk, sk, consistentRead, projectionExpression);
    }

    /**
     * Batch gets entities by exact keys (auto-splits into batches of 100, parallel execution with retry).
     * 按精确主键批量获取实体（自动按 100 条分批，并行执行，带重试机制）。
     *
     * @param clazz the entity class / 实体类
     * @param keys  the list of key pairs / 主键对列表
     * @param <T>   the entity type / 实体类型
     * @return the list of found entities / 找到的实体列表
     * @throws com.jojo.framework.easydynamodb.exception.DynamoBatchException if partial failures remain after retries / 重试后仍有部分失败时抛出
     */
    public <T> List<T> getBatch(Class<T> clazz, List<KeyPair> keys) {
        return batchOperation.getBatch(clazz, keys);
    }

    // ======== Query (条件查询) ========

    /**
     * Creates a fluent query builder for the given entity class.
     * 为指定实体类创建流式查询构建器。
     *
     * @param clazz the entity class / 实体类
     * @param <T>   the entity type / 实体类型
     * @return a QueryBuilder for chaining query conditions / 用于链式构建查询条件的 QueryBuilder
     */
    public <T> QueryOperation.QueryBuilder<T> query(Class<T> clazz) {
        return queryOperation.query(clazz);
    }

    /**
     * Creates a fluent scan builder for the given entity class.
     * 为指定实体类创建流式全表扫描构建器。
     *
     * @param clazz the entity class / 实体类
     * @param <T>   the entity type / 实体类型
     * @return a ScanBuilder for chaining scan conditions / 用于链式构建扫描条件的 ScanBuilder
     */
    public <T> ScanOperation.ScanBuilder<T> scan(Class<T> clazz) {
        return scanOperation.scan(clazz);
    }

    // ======== Update (更新) ========

    /**
     * Partial update: only updates fields touched by the mutator (auto-diff).
     * 部分更新：仅更新 mutator 中修改的字段（自动差异检测）。
     *
     * @param entity  the entity with key values set / 已设置主键值的实体
     * @param mutator a consumer that modifies the fields to update / 修改要更新字段的 Consumer
     * @param <T>     the entity type / 实体类型
     */
    public <T> void update(T entity, Consumer<T> mutator) {
        updateOperation.update(entity, mutator);
    }

    /**
     * Partial update with a condition expression.
     * 带条件表达式的部分更新。
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
     * @param entity    the entity with key values set / 已设置主键值的实体
     * @param mutator   a consumer that modifies the fields to update / 修改要更新字段的 Consumer
     * @param condition the condition that must be met / 必须满足的条件
     * @param <T>       the entity type / 实体类型
     * @throws com.jojo.framework.easydynamodb.exception.DynamoConditionFailedException if condition fails / 条件不满足时抛出
     */
    public <T> void update(T entity, Consumer<T> mutator, ConditionExpression condition) {
        updateOperation.update(entity, mutator, condition);
    }

    /**
     * Full update: SET all non-null fields, REMOVE all null fields.
     * 全量更新：非 null 字段执行 SET，null 字段执行 REMOVE。
     *
     * @param entity the entity to update / 要更新的实体
     * @param <T>    the entity type / 实体类型
     */
    public <T> void updateAll(T entity) {
        updateOperation.updateAll(entity);
    }

    /**
     * Full update with a condition expression.
     * 带条件表达式的全量更新。
     *
     * @param entity    the entity to update / 要更新的实体
     * @param condition the condition that must be met / 必须满足的条件
     * @param <T>       the entity type / 实体类型
     * @throws com.jojo.framework.easydynamodb.exception.DynamoConditionFailedException if condition fails / 条件不满足时抛出
     */
    public <T> void updateAll(T entity, ConditionExpression condition) {
        updateOperation.updateAll(entity, condition);
    }

    /**
     * Batch partial update: applies the same mutator to each entity in parallel.
     * 批量部分更新：对每个实体并行应用相同的 mutator。
     *
     * @param entities the list of entities to update / 要更新的实体列表
     * @param mutator  a consumer that modifies the fields to update / 修改要更新字段的 Consumer
     * @param <T>      the entity type / 实体类型
     * @throws com.jojo.framework.easydynamodb.exception.DynamoBatchException if partial failures occur / 部分失败时抛出
     */
    public <T> void updateBatch(List<T> entities, Consumer<T> mutator) {
        updateOperation.updateBatch(entities, mutator);
    }

    /**
     * Batch full update: updateAll for each entity in parallel.
     * 批量全量更新：对每个实体并行执行 updateAll。
     *
     * @param entities the list of entities to update / 要更新的实体列表
     * @param <T>      the entity type / 实体类型
     * @throws com.jojo.framework.easydynamodb.exception.DynamoBatchException if partial failures occur / 部分失败时抛出
     */
    public <T> void updateAllBatch(List<T> entities) {
        updateOperation.updateAllBatch(entities);
    }

    // ======== Expression Update (表达式更新 — 原子操作) ========

    /**
     * Creates an expression-based update builder for atomic operations (single partition key).
     * 创建基于表达式的更新构建器，用于原子操作（仅分区键）。
     * <p>
     * Use this for atomic increments/decrements, raw SET/REMOVE/ADD/DELETE
     * expressions, and conditional updates that go beyond the entity-level diff-based update.
     * <br>
     * 用于原子递增/递减、原始 SET/REMOVE/ADD/DELETE 表达式，以及超出实体级 diff 更新能力的条件更新。
     *
     * <pre>{@code
     * ddm.expressionUpdate(User.class, "user-001")
     *     .increment("coins", 100)
     *     .condition("#coins >= :min", c -> c.name("#coins", "coins").value(":min", 0))
     *     .execute();
     * }</pre>
     *
     * @param clazz        the entity class / 实体类
     * @param partitionKey the partition key value / 分区键值
     * @param <T>          the entity type / 实体类型
     * @return a fluent expression update builder / 流式表达式更新构建器
     */
    public <T> ExpressionUpdateOperation.ExpressionUpdateBuilder<T> expressionUpdate(Class<T> clazz, Object partitionKey) {
        return expressionUpdateOperation.expressionUpdate(clazz, partitionKey);
    }

    /**
     * Creates an expression-based update builder with composite key (partition key + sort key).
     * 创建基于表达式的更新构建器，支持复合主键（分区键 + 排序键）。
     *
     * @param clazz        the entity class / 实体类
     * @param partitionKey the partition key value / 分区键值
     * @param sortKey      the sort key value / 排序键值
     * @param <T>          the entity type / 实体类型
     * @return a fluent expression update builder / 流式表达式更新构建器
     */
    public <T> ExpressionUpdateOperation.ExpressionUpdateBuilder<T> expressionUpdate(Class<T> clazz, Object partitionKey, Object sortKey) {
        return expressionUpdateOperation.expressionUpdate(clazz, partitionKey, sortKey);
    }

    // ======== Delete (删除) ========

    /**
     * Deletes a single entity by partition key.
     * 通过分区键删除单个实体。
     *
     * @param clazz        the entity class / 实体类
     * @param partitionKey the partition key value / 分区键值
     * @param <T>          the entity type / 实体类型
     */
    public <T> void delete(Class<T> clazz, Object partitionKey) {
        deleteOperation.delete(clazz, partitionKey);
    }

    /**
     * Deletes a single entity by partition key + sort key.
     * 通过分区键 + 排序键删除单个实体。
     *
     * @param clazz the entity class / 实体类
     * @param pk    the partition key value / 分区键值
     * @param sk    the sort key value / 排序键值
     * @param <T>   the entity type / 实体类型
     */
    public <T> void delete(Class<T> clazz, Object pk, Object sk) {
        deleteOperation.delete(clazz, pk, sk);
    }

    /**
     * Deletes a single entity with a condition expression.
     * 带条件表达式删除单个实体。
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
     * @param clazz     the entity class / 实体类
     * @param pk        the partition key value / 分区键值
     * @param sk        the sort key value (nullable) / 排序键值（可为 null）
     * @param condition the condition expression / 条件表达式
     * @param <T>       the entity type / 实体类型
     * @throws com.jojo.framework.easydynamodb.exception.DynamoConditionFailedException if condition fails / 条件不满足时抛出
     */
    public <T> void delete(Class<T> clazz, Object pk, Object sk, ConditionExpression condition) {
        deleteOperation.delete(clazz, pk, sk, condition);
    }

    /**
     * Batch deletes by exact keys (auto-splits into batches of 25, parallel execution with retry).
     * 按精确主键批量删除（自动按 25 条分批，并行执行，带重试机制）。
     *
     * @param clazz the entity class / 实体类
     * @param keys  the list of key pairs to delete / 要删除的主键对列表
     * @param <T>   the entity type / 实体类型
     * @throws com.jojo.framework.easydynamodb.exception.DynamoBatchException if partial failures remain after retries / 重试后仍有部分失败时抛出
     */
    public <T> void deleteBatch(Class<T> clazz, List<KeyPair> keys) {
        batchOperation.deleteBatch(clazz, keys);
    }

    /**
     * Deletes all items matching a condition. Returns the number of items deleted.
     * 删除所有匹配条件的项。返回删除的项数。
     * <p>
     * Internally performs: scan with filter → extract keys → batch delete.
     * 内部执行流程：带过滤条件的 scan → 提取主键 → 批量删除。
     *
     * @param clazz            the entity class / 实体类
     * @param filterExpression the filter expression (e.g. "rating < :minRating") / 过滤表达式
     * @param expressionValues the expression attribute values / 表达式属性值
     * @param <T>              the entity type / 实体类型
     * @return the number of items deleted / 删除的项数
     */
    public <T> int deleteByCondition(Class<T> clazz,
                                     String filterExpression,
                                     Map<String, AttributeValue> expressionValues) {
        return deleteOperation.deleteByCondition(clazz, filterExpression, expressionValues, null);
    }

    /**
     * Deletes all items matching a condition with expression names. Returns the number of items deleted.
     * 删除所有匹配条件的项（支持表达式属性名）。返回删除的项数。
     *
     * @param clazz            the entity class / 实体类
     * @param filterExpression the filter expression / 过滤表达式
     * @param expressionValues the expression attribute values / 表达式属性值
     * @param expressionNames  the expression attribute names (nullable) / 表达式属性名（可为 null）
     * @param <T>              the entity type / 实体类型
     * @return the number of items deleted / 删除的项数
     */
    public <T> int deleteByCondition(Class<T> clazz,
                                     String filterExpression,
                                     Map<String, AttributeValue> expressionValues,
                                     Map<String, String> expressionNames) {
        return deleteOperation.deleteByCondition(clazz, filterExpression, expressionValues, expressionNames);
    }

    /**
     * Deletes all items matching a condition using auto-converted values (recommended).
     * 使用自动转换值删除所有匹配条件的项（推荐方式）。
     * <p>
     * Values are automatically converted to AttributeValue using the same rules
     * as {@code query().value()} — supports String, Number, Boolean, Enum, Instant, etc.
     * <br>
     * 值会自动转换为 AttributeValue，转换规则与 {@code query().value()} 相同。
     *
     * <pre>{@code
     * int deleted = ddm.deleteByConditionWithValues(Game.class,
     *     "rating < :minRating",
     *     Map.of(":minRating", 5.0));
     * }</pre>
     *
     * @param clazz            the entity class / 实体类
     * @param filterExpression the filter expression / 过滤表达式
     * @param values           the expression values (auto-converted) / 表达式值（自动转换）
     * @param <T>              the entity type / 实体类型
     * @return the number of items deleted / 删除的项数
     */
    public <T> int deleteByConditionWithValues(Class<T> clazz,
                                               String filterExpression,
                                               Map<String, Object> values) {
        return deleteByConditionWithValues(clazz, filterExpression, values, null);
    }

    /**
     * Deletes all items matching a condition using auto-converted values and expression names.
     * 使用自动转换值和表达式属性名删除所有匹配条件的项。
     *
     * @param clazz            the entity class / 实体类
     * @param filterExpression the filter expression / 过滤表达式
     * @param values           the expression values (auto-converted) / 表达式值（自动转换）
     * @param expressionNames  the expression attribute names (nullable) / 表达式属性名（可为 null）
     * @param <T>              the entity type / 实体类型
     * @return the number of items deleted / 删除的项数
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
     * 创建事务写入构建器，用于原子性多项操作。
     * <p>
     * A transaction guarantees all-or-nothing: either all items succeed or none are applied.
     * Supports up to 100 items per transaction.
     * <br>
     * 事务保证全部成功或全部不生效。每个事务最多支持 100 个项。
     *
     * @return a fluent transaction write builder / 流式事务写入构建器
     */
    public TransactionOperation.TransactWriteBuilder transact() {
        return transactionOperation.transact();
    }

    /**
     * Creates a transaction read builder for consistent multi-item reads.
     * 创建事务读取构建器，用于一致性多项读取。
     * <p>
     * Guarantees a consistent snapshot across all items at the time of the read.
     * <br>
     * 保证读取时所有项的一致性快照。
     *
     * @return a fluent transaction read builder / 流式事务读取构建器
     */
    public TransactionOperation.TransactGetBuilder transactGet() {
        return transactionOperation.transactGet();
    }

    // ======== Builder ========

    /**
     * Builder for constructing DDM instances with custom configuration.
     * 用于构建自定义配置的 DDM 实例的构建器。
     */
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

        /**
         * Sets the table name prefix prepended to all table names.
         * 设置表名前缀，拼接在所有表名之前。
         *
         * @param prefix the prefix string (null treated as empty) / 前缀字符串（null 视为空）
         * @return this builder / 当前构建器
         */
        public Builder tablePrefix(String prefix) {
            this.tablePrefix = prefix == null ? "" : prefix;
            return this;
        }

        /**
         * Enables auto-creation of DynamoDB tables on first save if they don't exist.
         * 启用首次 save 时自动创建不存在的 DynamoDB 表。
         *
         * @param enabled true to enable auto-creation (PAY_PER_REQUEST billing) / true 启用自动建表
         * @return this builder / 当前构建器
         */
        public Builder autoCreateTable(boolean enabled) {
            this.autoCreateTable = enabled;
            return this;
        }

        /**
         * Sets a custom table name resolver for advanced naming strategies (e.g. multi-tenant).
         * 设置自定义表名解析器，用于高级命名策略（如多租户）。
         *
         * @param resolver the custom resolver / 自定义解析器
         * @return this builder / 当前构建器
         */
        public Builder tableNameResolver(TableNameResolver resolver) {
            this.tableNameResolver = resolver;
            return this;
        }

        /**
         * Sets a custom executor for batch parallel operations (default: virtual threads).
         * 设置批量并行操作的自定义执行器（默认：虚拟线程）。
         *
         * @param executor the custom executor / 自定义执行器
         * @return this builder / 当前构建器
         */
        public Builder batchExecutor(Executor executor) {
            this.batchExecutor = executor;
            return this;
        }

        /**
         * Registers a global custom converter for a specific type.
         * 为指定类型注册全局自定义转换器。
         *
         * @param type      the Java class to associate / 要关联的 Java 类
         * @param converter the converter instance / 转换器实例
         * @return this builder / 当前构建器
         */
        public Builder registerConverter(Class<?> type, AttributeConverter<?> converter) {
            converterRegistry.register(type, converter);
            return this;
        }

        /**
         * Pre-registers entity classes at build time (optional, auto-registered on first use).
         * 在构建时预注册实体类（可选，首次使用时也会自动注册）。
         *
         * @param entityClasses the entity classes to register / 要注册的实体类
         * @return this builder / 当前构建器
         */
        public Builder register(Class<?>... entityClasses) {
            this.pendingEntityClasses = entityClasses;
            return this;
        }

        /**
         * Enables or disables logging for all EasyDynamodb operations.
         * Disabled by default. When enabled, uses SLF4J with the configured log level.
         * <br>
         * 启用或禁用所有 EasyDynamodb 操作的日志。默认禁用。启用后使用 SLF4J 和配置的日志级别。
         *
         * @param enabled true to enable logging / true 启用日志
         * @return this builder / 当前构建器
         */
        public Builder enableLogging(boolean enabled) {
            this.enableLogging = enabled;
            return this;
        }

        /**
         * Sets the minimum log level. Default is INFO. Only effective when logging is enabled.
         * 设置最低日志级别。默认 INFO。仅在日志启用时生效。
         *
         * @param level one of TRACE, DEBUG, INFO, WARN, ERROR / TRACE、DEBUG、INFO、WARN、ERROR 之一
         * @return this builder / 当前构建器
         */
        public Builder logLevel(Level level) {
            this.logLevel = level;
            return this;
        }

        /**
         * Builds and returns the DDM instance with all configured options.
         * 使用所有已配置的选项构建并返回 DDM 实例。
         *
         * @return a new DDM instance / 新的 DDM 实例
         */
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
