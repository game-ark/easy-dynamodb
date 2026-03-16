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
 * 处理 DynamoDB TransactWriteItems 和 TransactGetItems 操作。
 * <p>
 * Supports up to 100 items per transaction (DynamoDB limit), with automatic
 * splitting into multiple transactions if needed (opt-in).
 * 每个事务最多支持 100 个项（DynamoDB 限制），如需可自动拆分为多个事务（可选）。
 * <p>
 * A transaction guarantees all-or-nothing atomicity: either all items succeed
 * or none are applied. This is essential for operations like:
 * 事务保证全有或全无的原子性：要么所有项成功，要么全部不生效。适用于以下操作：
 * <ul>
 *   <li>Transferring currency between two players / 在两个玩家之间转移货币</li>
 *   <li>Deducting items while granting rewards / 扣除物品同时发放奖励</li>
 *   <li>Creating related records across multiple tables atomically / 跨多个表原子性地创建关联记录</li>
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

    /**
     * Constructs a TransactionOperation.
     * 构造 TransactionOperation。
     *
     * @param dynamoDbClient   the DynamoDB client / DynamoDB 客户端
     * @param metadataRegistry the metadata registry / 元数据注册中心
     * @param saveOperation    the save operation for entity conversion / 用于实体转换的保存操作
     */
    public TransactionOperation(DynamoDbClient dynamoDbClient,
                                MetadataRegistry metadataRegistry,
                                SaveOperation saveOperation) {
        this.dynamoDbClient = dynamoDbClient;
        this.metadataRegistry = metadataRegistry;
        this.saveOperation = saveOperation;
    }

    /**
     * Creates a new transaction write builder.
     * 创建新的事务写入构建器。
     *
     * @return a new TransactWriteBuilder / 新的 TransactWriteBuilder
     */
    public TransactWriteBuilder transact() {
        return new TransactWriteBuilder();
    }

    /**
     * Creates a new transaction read builder.
     * 创建新的事务读取构建器。
     *
     * @return a new TransactGetBuilder / 新的 TransactGetBuilder
     */
    public TransactGetBuilder transactGet() {
        return new TransactGetBuilder();
    }

    // ========================================================================
    // TransactWriteItems Builder
    // ========================================================================

    /**
     * Fluent builder for constructing DynamoDB TransactWriteItems requests.
     * 用于构建 DynamoDB TransactWriteItems 请求的流式构建器。
     * <p>
     * Supports four action types per item:
     * 每个项支持四种操作类型：
     * <ul>
     *   <li>{@code put} — insert or replace an item / 插入或替换项</li>
     *   <li>{@code update} — expression-based update / 基于表达式的更新</li>
     *   <li>{@code delete} — remove an item / 删除项</li>
     *   <li>{@code conditionCheck} — validate a condition without modifying data / 验证条件但不修改数据</li>
     * </ul>
     */
    public class TransactWriteBuilder {
        private final List<TransactWriteItem> items = new ArrayList<>();
        private String clientRequestToken;

        // ---- Put ----

        /**
         * Adds a Put action to the transaction.
         * 向事务添加 Put 操作。
         *
         * @param entity the entity to put / 要放入的实体
         * @param <T>    the entity type / 实体类型
         * @return this builder / 当前构建器
         */
        public <T> TransactWriteBuilder put(T entity) {
            return put(entity, null);
        }

        /**
         * Adds a Put action with a condition expression.
         * 向事务添加带条件表达式的 Put 操作。
         *
         * @param entity    the entity to put / 要放入的实体
         * @param condition the condition that must be met (nullable) / 必须满足的条件（可为 null）
         * @param <T>       the entity type / 实体类型
         * @return this builder / 当前构建器
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
         * 向事务添加基于表达式的 Update 操作。
         * <p>
         * The configurator receives an {@link TransactUpdateBuilder} to build
         * the update expression with SET/REMOVE/ADD/DELETE clauses and conditions.
         * configurator 接收 {@link TransactUpdateBuilder} 以构建包含 SET/REMOVE/ADD/DELETE 子句和条件的更新表达式。
         *
         * @param clazz        the entity class / 实体类
         * @param partitionKey the partition key value / 分区键值
         * @param configurator callback to configure the update expression / 用于配置更新表达式的回调
         * @param <T>          the entity type / 实体类型
         * @return this builder / 当前构建器
         */
        public <T> TransactWriteBuilder update(Class<T> clazz, Object partitionKey,
                                                Consumer<TransactUpdateBuilder> configurator) {
            return update(clazz, partitionKey, null, configurator);
        }

        /**
         * Adds an expression-based Update action with composite key.
         * 向事务添加带复合键的基于表达式的 Update 操作。
         *
         * @param clazz        the entity class / 实体类
         * @param partitionKey the partition key value / 分区键值
         * @param sortKey      the sort key value (nullable) / 排序键值（可为 null）
         * @param configurator callback to configure the update expression / 用于配置更新表达式的回调
         * @param <T>          the entity type / 实体类型
         * @return this builder / 当前构建器
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
         * 向事务添加 Delete 操作。
         *
         * @param clazz        the entity class / 实体类
         * @param partitionKey the partition key value / 分区键值
         * @param <T>          the entity type / 实体类型
         * @return this builder / 当前构建器
         */
        public <T> TransactWriteBuilder delete(Class<T> clazz, Object partitionKey) {
            return delete(clazz, partitionKey, null, null);
        }

        /**
         * Adds a Delete action with composite key.
         * 向事务添加带复合键的 Delete 操作。
         *
         * @param clazz        the entity class / 实体类
         * @param partitionKey the partition key value / 分区键值
         * @param sortKey      the sort key value / 排序键值
         * @param <T>          the entity type / 实体类型
         * @return this builder / 当前构建器
         */
        public <T> TransactWriteBuilder delete(Class<T> clazz, Object partitionKey, Object sortKey) {
            return delete(clazz, partitionKey, sortKey, null);
        }

        /**
         * Adds a Delete action with condition expression.
         * 向事务添加带条件表达式的 Delete 操作。
         *
         * @param clazz        the entity class / 实体类
         * @param partitionKey the partition key value / 分区键值
         * @param sortKey      the sort key value (nullable) / 排序键值（可为 null）
         * @param condition    the condition expression (nullable) / 条件表达式（可为 null）
         * @param <T>          the entity type / 实体类型
         * @return this builder / 当前构建器
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
         * 添加 ConditionCheck 操作——验证条件但不修改数据。
         * <p>
         * Useful for ensuring preconditions are met before the transaction commits.
         * 适用于确保事务提交前满足前置条件。
         *
         * @param clazz        the entity class / 实体类
         * @param partitionKey the partition key value / 分区键值
         * @param condition    the condition that must be satisfied / 必须满足的条件
         * @param <T>          the entity type / 实体类型
         * @return this builder / 当前构建器
         */
        public <T> TransactWriteBuilder conditionCheck(Class<T> clazz, Object partitionKey,
                                                        ConditionExpression condition) {
            return conditionCheck(clazz, partitionKey, null, condition);
        }

        /**
         * Adds a ConditionCheck action with composite key.
         * 添加带复合键的 ConditionCheck 操作。
         *
         * @param clazz        the entity class / 实体类
         * @param partitionKey the partition key value / 分区键值
         * @param sortKey      the sort key value (nullable) / 排序键值（可为 null）
         * @param condition    the condition that must be satisfied / 必须满足的条件
         * @param <T>          the entity type / 实体类型
         * @return this builder / 当前构建器
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
         * 设置客户端请求令牌以实现幂等性。
         * <p>
         * If the same token is used within 10 minutes, DynamoDB returns the
         * previous result without re-executing the transaction.
         * 如果在 10 分钟内使用相同的令牌，DynamoDB 将返回之前的结果而不重新执行事务。
         *
         * @param token the idempotency token / 幂等性令牌
         * @return this builder / 当前构建器
         */
        public TransactWriteBuilder idempotencyToken(String token) {
            this.clientRequestToken = token;
            return this;
        }

        // ---- Execute ----

        /**
         * Executes the transaction. All items succeed or none are applied.
         * 执行事务。所有项要么全部成功，要么全部不生效。
         *
         * @throws DynamoTransactionException if the transaction is cancelled / 事务被取消时抛出
         * @throws DynamoConditionFailedException if a condition check fails / 条件检查失败时抛出
         * @throws DynamoException if the transaction fails for other reasons / 事务因其他原因失败时抛出
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
     * 用于在事务中构建 Update 操作的构建器。
     * 镜像 {@link ExpressionUpdateOperation.ExpressionUpdateBuilder} 的表达式能力，
     * 但生成用于事务的 DynamoDB {@link Update} 对象。
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

        /**
         * Adds a raw SET clause.
         * 添加原始 SET 子句。
         *
         * @param setExpression the SET expression fragment / SET 表达式片段
         * @return this builder / 当前构建器
         */
        public TransactUpdateBuilder set(String setExpression) {
            setClauses.add(setExpression);
            return this;
        }

        /**
         * Convenience: SET a single attribute to a value.
         * 便捷方法：将单个属性设置为指定值。
         *
         * @param attributeName the DynamoDB attribute name / DynamoDB 属性名
         * @param value         the Java value (auto-converted) / Java 值（自动转换）
         * @return this builder / 当前构建器
         */
        public TransactUpdateBuilder set(String attributeName, Object value) {
            String nameAlias = "#sa" + setClauses.size();
            String valueAlias = ":sv" + setClauses.size();
            expressionNames.put(nameAlias, attributeName);
            expressionValues.put(valueAlias, AttributeValues.of(value));
            setClauses.add(nameAlias + " = " + valueAlias);
            return this;
        }

        /**
         * Atomic increment. Generates {@code SET #attr = #attr + :val}.
         * 原子递增。生成 {@code SET #attr = #attr + :val}。
         *
         * @param attributeName the DynamoDB attribute name / DynamoDB 属性名
         * @param amount        the amount to add / 要增加的数量
         * @return this builder / 当前构建器
         */
        public TransactUpdateBuilder increment(String attributeName, Number amount) {
            String nameAlias = "#inc" + setClauses.size();
            String valueAlias = ":inc" + setClauses.size();
            expressionNames.put(nameAlias, attributeName);
            expressionValues.put(valueAlias, AttributeValues.of(amount));
            setClauses.add(nameAlias + " = " + nameAlias + " + " + valueAlias);
            return this;
        }

        /**
         * Atomic decrement. Generates {@code SET #attr = #attr - :val}.
         * 原子递减。生成 {@code SET #attr = #attr - :val}。
         *
         * @param attributeName the DynamoDB attribute name / DynamoDB 属性名
         * @param amount        the positive amount to subtract / 要减去的正数数量
         * @return this builder / 当前构建器
         */
        public TransactUpdateBuilder decrement(String attributeName, Number amount) {
            String nameAlias = "#dec" + setClauses.size();
            String valueAlias = ":dec" + setClauses.size();
            expressionNames.put(nameAlias, attributeName);
            expressionValues.put(valueAlias, AttributeValues.of(amount));
            setClauses.add(nameAlias + " = " + nameAlias + " - " + valueAlias);
            return this;
        }

        /**
         * Adds a REMOVE clause for one or more attributes.
         * 为一个或多个属性添加 REMOVE 子句。
         *
         * @param attributeNames the attribute names to remove / 要移除的属性名
         * @return this builder / 当前构建器
         */
        public TransactUpdateBuilder remove(String... attributeNames) {
            for (String attr : attributeNames) {
                String nameAlias = "#rm" + removeClauses.size();
                expressionNames.put(nameAlias, attr);
                removeClauses.add(nameAlias);
            }
            return this;
        }

        /**
         * Adds an ADD clause for a number or set attribute.
         * 为数字或集合属性添加 ADD 子句。
         *
         * @param attributeName the attribute name / 属性名
         * @param value         the value to add / 要添加的值
         * @return this builder / 当前构建器
         */
        public TransactUpdateBuilder add(String attributeName, Object value) {
            String nameAlias = "#ad" + addClauses.size();
            String valueAlias = ":ad" + addClauses.size();
            expressionNames.put(nameAlias, attributeName);
            expressionValues.put(valueAlias, AttributeValues.of(value));
            addClauses.add(nameAlias + " " + valueAlias);
            return this;
        }

        /**
         * Adds an expression attribute name mapping.
         * 添加表达式属性名映射。
         *
         * @param placeholder   the placeholder (e.g. "#status") / 占位符（例如 "#status"）
         * @param attributeName the actual attribute name / 实际属性名
         * @return this builder / 当前构建器
         */
        public TransactUpdateBuilder name(String placeholder, String attributeName) {
            expressionNames.put(placeholder, attributeName);
            return this;
        }

        /**
         * Adds an expression attribute value with auto-conversion.
         * 添加表达式属性值，自动转换类型。
         *
         * @param placeholder the placeholder (e.g. ":amount") / 占位符（例如 ":amount"）
         * @param val         the Java value / Java 值
         * @return this builder / 当前构建器
         */
        public TransactUpdateBuilder value(String placeholder, Object val) {
            expressionValues.put(placeholder, AttributeValues.of(val));
            return this;
        }

        /**
         * Adds a raw AttributeValue expression value.
         * 添加原始 AttributeValue 表达式值。
         *
         * @param placeholder    the placeholder / 占位符
         * @param attributeValue the raw AttributeValue / 原始 AttributeValue
         * @return this builder / 当前构建器
         */
        public TransactUpdateBuilder rawValue(String placeholder, AttributeValue attributeValue) {
            expressionValues.put(placeholder, attributeValue);
            return this;
        }

        /**
         * Sets a pre-built condition expression.
         * 设置预构建的条件表达式。
         *
         * @param condition the condition expression / 条件表达式
         * @return this builder / 当前构建器
         */
        public TransactUpdateBuilder condition(ConditionExpression condition) {
            this.conditionExpression = condition;
            return this;
        }

        /**
         * Sets a simple condition expression string.
         * 设置简单的条件表达式字符串。
         *
         * @param expression the condition expression string / 条件表达式字符串
         * @return this builder / 当前构建器
         */
        public TransactUpdateBuilder condition(String expression) {
            this.conditionExpression = ConditionExpression.of(expression);
            return this;
        }

        /**
         * Sets a condition expression with a builder callback for names/values.
         * 设置条件表达式，通过构建器回调配置名称/值。
         *
         * @param expression   the condition expression string / 条件表达式字符串
         * @param configurator callback to configure names and values / 用于配置名称和值的回调
         * @return this builder / 当前构建器
         */
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
     * 用于构建 DynamoDB TransactGetItems 请求的流式构建器。
     * <p>
     * Reads up to 100 items atomically — guarantees a consistent snapshot
     * across all items at the time of the read.
     * 原子性地读取最多 100 个项——保证读取时所有项的一致性快照。
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
         * 通过分区键添加 Get 操作。
         *
         * @param clazz        the entity class / 实体类
         * @param partitionKey the partition key value / 分区键值
         * @param <T>          the entity type / 实体类型
         * @return this builder / 当前构建器
         */
        public <T> TransactGetBuilder get(Class<T> clazz, Object partitionKey) {
            return get(clazz, partitionKey, null);
        }

        /**
         * Adds a Get action by composite key.
         * 通过复合键添加 Get 操作。
         *
         * @param clazz        the entity class / 实体类
         * @param partitionKey the partition key value / 分区键值
         * @param sortKey      the sort key value (nullable) / 排序键值（可为 null）
         * @param <T>          the entity type / 实体类型
         * @return this builder / 当前构建器
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
         * 添加带投影的 Get 操作（仅返回特定属性）。
         *
         * @param clazz               the entity class / 实体类
         * @param partitionKey         the partition key value / 分区键值
         * @param sortKey              the sort key value (nullable) / 排序键值（可为 null）
         * @param projectionExpression the projection expression / 投影表达式
         * @param <T>                  the entity type / 实体类型
         * @return this builder / 当前构建器
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
         * 执行事务性读取。返回所有项的一致性快照。
         *
         * @return the transaction get result / 事务读取结果
         * @throws DynamoTransactionException if the transaction fails / 事务失败时抛出
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
     * TransactGetItems 操作的结果。项按添加到构建器的顺序返回。
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
         * 获取指定索引处的项，转换为期望的实体类型。
         *
         * @param index the 0-based index (same order as builder calls) / 从 0 开始的索引（与构建器调用顺序一致）
         * @param clazz the expected entity class / 期望的实体类
         * @param <T>   the entity type / 实体类型
         * @return the entity, or null if the item was not found / 实体，未找到时返回 null
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
         * 返回结果中的项数。
         *
         * @return the number of items / 项数
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
