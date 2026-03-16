package com.jojo.framework.easydynamodb.operation;

import com.jojo.framework.easydynamodb.exception.DynamoConditionFailedException;
import com.jojo.framework.easydynamodb.exception.DynamoException;
import com.jojo.framework.easydynamodb.logging.DdmLogger;
import com.jojo.framework.easydynamodb.metadata.EntityMetadata;
import com.jojo.framework.easydynamodb.metadata.MetadataRegistry;
import com.jojo.framework.easydynamodb.model.ConditionExpression;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.*;

/**
 * Handles expression-based update operations — atomic increments, decrements,
 * raw SET/REMOVE/ADD/DELETE expressions, and conditional updates.
 * 处理基于表达式的更新操作——原子递增、递减、原始 SET/REMOVE/ADD/DELETE 表达式以及条件更新。
 * <p>
 * This complements {@link UpdateOperation} (which does entity-level diff-based updates)
 * by providing low-level expression control for scenarios like:
 * 这是对 {@link UpdateOperation}（基于实体级别差异的更新）的补充，
 * 为以下场景提供底层表达式控制：
 * <ul>
 *   <li>Atomic counter increments: {@code SET coins = coins + :amount} / 原子计数器递增</li>
 *   <li>Conditional updates: {@code SET status = :new IF status = :old} / 条件更新</li>
 *   <li>Adding to sets: {@code ADD tags :newTags} / 向集合添加元素</li>
 *   <li>Removing attributes: {@code REMOVE tempFlag} / 移除属性</li>
 * </ul>
 *
 * <pre>{@code
 * ddm.expressionUpdate(User.class, "user-001")
 *     .set("#coins = #coins + :amount")
 *     .name("#coins", "coins")
 *     .value(":amount", 100)
 *     .condition("#coins >= :min", c -> c.name("#coins", "coins").value(":min", 0))
 *     .execute();
 * }</pre>
 */
public class ExpressionUpdateOperation {

    private static final DdmLogger log = DdmLogger.getLogger(ExpressionUpdateOperation.class);

    private final DynamoDbClient dynamoDbClient;
    private final MetadataRegistry metadataRegistry;

    /**
     * Constructs an ExpressionUpdateOperation.
     * 构造 ExpressionUpdateOperation。
     *
     * @param dynamoDbClient   the DynamoDB client / DynamoDB 客户端
     * @param metadataRegistry the metadata registry / 元数据注册中心
     */
    public ExpressionUpdateOperation(DynamoDbClient dynamoDbClient, MetadataRegistry metadataRegistry) {
        this.dynamoDbClient = dynamoDbClient;
        this.metadataRegistry = metadataRegistry;
    }

    /**
     * Creates a new ExpressionUpdateBuilder for the given entity class and primary key.
     * 为给定的实体类和主键创建新的 ExpressionUpdateBuilder。
     *
     * @param clazz        the entity class / 实体类
     * @param partitionKey the partition key value / 分区键值
     * @param <T>          the entity type / 实体类型
     * @return a new builder / 新的构建器
     */
    public <T> ExpressionUpdateBuilder<T> expressionUpdate(Class<T> clazz, Object partitionKey) {
        return expressionUpdate(clazz, partitionKey, null);
    }

    /**
     * Creates a new ExpressionUpdateBuilder for the given entity class and composite key.
     * 为给定的实体类和复合键创建新的 ExpressionUpdateBuilder。
     *
     * @param clazz        the entity class / 实体类
     * @param partitionKey the partition key value / 分区键值
     * @param sortKey      the sort key value (nullable) / 排序键值（可为 null）
     * @param <T>          the entity type / 实体类型
     * @return a new builder / 新的构建器
     */
    public <T> ExpressionUpdateBuilder<T> expressionUpdate(Class<T> clazz, Object partitionKey, Object sortKey) {
        metadataRegistry.register(clazz);
        EntityMetadata metadata = metadataRegistry.getMetadata(clazz);
        return new ExpressionUpdateBuilder<>(clazz, metadata, partitionKey, sortKey);
    }

    /**
     * Fluent builder for constructing expression-based DynamoDB UpdateItem requests.
     * 用于构建基于表达式的 DynamoDB UpdateItem 请求的流式构建器。
     * <p>
     * Supports all four DynamoDB update expression clauses:
     * 支持所有四种 DynamoDB 更新表达式子句：
     * <ul>
     *   <li>{@code SET} — assign values, including arithmetic expressions / 赋值，包括算术表达式</li>
     *   <li>{@code REMOVE} — remove attributes from the item / 从项中移除属性</li>
     *   <li>{@code ADD} — add to number or set attributes / 向数字或集合属性添加值</li>
     *   <li>{@code DELETE} — remove elements from a set / 从集合中移除元素</li>
     * </ul>
     *
     * @param <T> the entity type / 实体类型
     */
    public class ExpressionUpdateBuilder<T> {
        private final Class<T> clazz;
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
        private ReturnValue returnValue;

        ExpressionUpdateBuilder(Class<T> clazz, EntityMetadata metadata,
                                Object partitionKey, Object sortKey) {
            this.clazz = clazz;
            this.metadata = metadata;
            this.partitionKey = partitionKey;
            this.sortKey = sortKey;
        }

        // ---- SET clause ----

        /**
         * Adds a raw SET clause. Multiple calls are joined with commas.
         * 添加原始 SET 子句。多次调用将以逗号连接。
         * <p>
         * Example: {@code .set("#coins = #coins + :amount")}
         *
         * @param setExpression the SET expression fragment / SET 表达式片段
         * @return this builder / 当前构建器
         */
        public ExpressionUpdateBuilder<T> set(String setExpression) {
            setClauses.add(setExpression);
            return this;
        }

        /**
         * Convenience: SET a single attribute to a value.
         * 便捷方法：将单个属性设置为指定值。
         * <p>
         * Example: {@code .set("status", "ACTIVE")} → {@code SET #f0 = :v0}
         *
         * @param attributeName the DynamoDB attribute name / DynamoDB 属性名
         * @param value         the Java value (auto-converted) / Java 值（自动转换）
         * @return this builder / 当前构建器
         */
        public ExpressionUpdateBuilder<T> set(String attributeName, Object value) {
            String nameAlias = "#sa" + setClauses.size();
            String valueAlias = ":sv" + setClauses.size();
            expressionNames.put(nameAlias, attributeName);
            expressionValues.put(valueAlias, AttributeValues.of(value));
            setClauses.add(nameAlias + " = " + valueAlias);
            return this;
        }

        /**
         * Convenience: atomic increment. Generates {@code SET #attr = #attr + :val}.
         * 便捷方法：原子递增。生成 {@code SET #attr = #attr + :val}。
         *
         * @param attributeName the DynamoDB attribute name / DynamoDB 属性名
         * @param amount        the amount to add (positive to increment, negative to decrement) / 要增加的数量（正数递增，负数递减）
         * @return this builder / 当前构建器
         */
        public ExpressionUpdateBuilder<T> increment(String attributeName, Number amount) {
            String nameAlias = "#inc" + setClauses.size();
            String valueAlias = ":inc" + setClauses.size();
            expressionNames.put(nameAlias, attributeName);
            expressionValues.put(valueAlias, AttributeValues.of(amount));
            setClauses.add(nameAlias + " = " + nameAlias + " + " + valueAlias);
            return this;
        }

        /**
         * Convenience: atomic decrement. Generates {@code SET #attr = #attr - :val}.
         * 便捷方法：原子递减。生成 {@code SET #attr = #attr - :val}。
         *
         * @param attributeName the DynamoDB attribute name / DynamoDB 属性名
         * @param amount        the positive amount to subtract / 要减去的正数数量
         * @return this builder / 当前构建器
         */
        public ExpressionUpdateBuilder<T> decrement(String attributeName, Number amount) {
            String nameAlias = "#dec" + setClauses.size();
            String valueAlias = ":dec" + setClauses.size();
            expressionNames.put(nameAlias, attributeName);
            expressionValues.put(valueAlias, AttributeValues.of(amount));
            setClauses.add(nameAlias + " = " + nameAlias + " - " + valueAlias);
            return this;
        }

        /**
         * Convenience: SET with if_not_exists. Generates
         * {@code SET #attr = if_not_exists(#attr, :default) + :val}.
         * 便捷方法：带 if_not_exists 的 SET。生成
         * {@code SET #attr = if_not_exists(#attr, :default) + :val}。
         * <p>
         * Useful for initializing a counter that may not exist yet.
         * 适用于初始化可能尚不存在的计数器。
         *
         * @param attributeName the DynamoDB attribute name / DynamoDB 属性名
         * @param defaultValue  the default value if attribute doesn't exist / 属性不存在时的默认值
         * @param addValue      the value to add / 要增加的值
         * @return this builder / 当前构建器
         */
        public ExpressionUpdateBuilder<T> incrementIfNotExists(String attributeName,
                                                                Number defaultValue,
                                                                Number addValue) {
            String nameAlias = "#ine" + setClauses.size();
            String defaultAlias = ":ined" + setClauses.size();
            String addAlias = ":inea" + setClauses.size();
            expressionNames.put(nameAlias, attributeName);
            expressionValues.put(defaultAlias, AttributeValues.of(defaultValue));
            expressionValues.put(addAlias, AttributeValues.of(addValue));
            setClauses.add(nameAlias + " = if_not_exists(" + nameAlias + ", " + defaultAlias + ") + " + addAlias);
            return this;
        }

        /**
         * Convenience: SET with list_append. Generates
         * {@code SET #attr = list_append(#attr, :val)} or
         * {@code SET #attr = list_append(:val, #attr)} depending on prepend flag.
         * 便捷方法：带 list_append 的 SET。根据 prepend 标志生成
         * {@code SET #attr = list_append(#attr, :val)} 或
         * {@code SET #attr = list_append(:val, #attr)}。
         *
         * @param attributeName the DynamoDB attribute name / DynamoDB 属性名
         * @param value         the list value to append/prepend (as AttributeValue L type) / 要追加/前置的列表值（AttributeValue L 类型）
         * @param prepend       true to prepend, false to append / true 为前置，false 为追加
         * @return this builder / 当前构建器
         */
        public ExpressionUpdateBuilder<T> listAppend(String attributeName, AttributeValue value, boolean prepend) {
            String nameAlias = "#la" + setClauses.size();
            String valueAlias = ":la" + setClauses.size();
            expressionNames.put(nameAlias, attributeName);
            expressionValues.put(valueAlias, value);
            if (prepend) {
                setClauses.add(nameAlias + " = list_append(" + valueAlias + ", " + nameAlias + ")");
            } else {
                setClauses.add(nameAlias + " = list_append(" + nameAlias + ", " + valueAlias + ")");
            }
            return this;
        }

        // ---- REMOVE clause ----

        /**
         * Adds a REMOVE clause for one or more attributes.
         * 为一个或多个属性添加 REMOVE 子句。
         * <p>
         * Example: {@code .remove("tempFlag", "oldField")}
         *
         * @param attributeNames the attribute names to remove / 要移除的属性名
         * @return this builder / 当前构建器
         */
        public ExpressionUpdateBuilder<T> remove(String... attributeNames) {
            for (String attr : attributeNames) {
                String nameAlias = "#rm" + removeClauses.size();
                expressionNames.put(nameAlias, attr);
                removeClauses.add(nameAlias);
            }
            return this;
        }

        /**
         * Adds a raw REMOVE clause fragment.
         * 添加原始 REMOVE 子句片段。
         *
         * @param removeExpression the REMOVE expression fragment / REMOVE 表达式片段
         * @return this builder / 当前构建器
         */
        public ExpressionUpdateBuilder<T> removeRaw(String removeExpression) {
            removeClauses.add(removeExpression);
            return this;
        }

        // ---- ADD clause ----

        /**
         * Adds an ADD clause — adds a value to a number attribute or adds elements to a set.
         * 添加 ADD 子句——向数字属性添加值或向集合添加元素。
         * <p>
         * Example: {@code .add("viewCount", 1)} or {@code .add("tags", setOfStrings)}
         *
         * @param attributeName the attribute name / 属性名
         * @param value         the value to add / 要添加的值
         * @return this builder / 当前构建器
         */
        public ExpressionUpdateBuilder<T> add(String attributeName, Object value) {
            String nameAlias = "#ad" + addClauses.size();
            String valueAlias = ":ad" + addClauses.size();
            expressionNames.put(nameAlias, attributeName);
            expressionValues.put(valueAlias, AttributeValues.of(value));
            addClauses.add(nameAlias + " " + valueAlias);
            return this;
        }

        /**
         * Adds a raw ADD clause fragment.
         * 添加原始 ADD 子句片段。
         *
         * @param addExpression the ADD expression fragment / ADD 表达式片段
         * @return this builder / 当前构建器
         */
        public ExpressionUpdateBuilder<T> addRaw(String addExpression) {
            addClauses.add(addExpression);
            return this;
        }

        // ---- DELETE clause (set element removal) ----

        /**
         * Adds a DELETE clause — removes elements from a set attribute.
         * 添加 DELETE 子句——从集合属性中移除元素。
         *
         * @param attributeName the set attribute name / 集合属性名
         * @param value         the set of elements to remove (as AttributeValue SS/NS/BS) / 要移除的元素集合（AttributeValue SS/NS/BS 类型）
         * @return this builder / 当前构建器
         */
        public ExpressionUpdateBuilder<T> deleteFromSet(String attributeName, AttributeValue value) {
            String nameAlias = "#dl" + deleteClauses.size();
            String valueAlias = ":dl" + deleteClauses.size();
            expressionNames.put(nameAlias, attributeName);
            expressionValues.put(valueAlias, value);
            deleteClauses.add(nameAlias + " " + valueAlias);
            return this;
        }

        // ---- Expression attribute names/values ----

        /**
         * Adds an expression attribute name mapping.
         * 添加表达式属性名映射。
         *
         * @param placeholder   the placeholder (e.g. "#status") / 占位符（例如 "#status"）
         * @param attributeName the actual attribute name (e.g. "status") / 实际属性名（例如 "status"）
         * @return this builder / 当前构建器
         */
        public ExpressionUpdateBuilder<T> name(String placeholder, String attributeName) {
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
        public ExpressionUpdateBuilder<T> value(String placeholder, Object val) {
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
        public ExpressionUpdateBuilder<T> rawValue(String placeholder, AttributeValue attributeValue) {
            expressionValues.put(placeholder, attributeValue);
            return this;
        }

        // ---- Condition expression ----

        /**
         * Sets a condition expression that must be satisfied for the update to proceed.
         * 设置更新必须满足的条件表达式。
         *
         * @param conditionExpression the pre-built condition expression / 预构建的条件表达式
         * @return this builder / 当前构建器
         */
        public ExpressionUpdateBuilder<T> condition(ConditionExpression conditionExpression) {
            this.conditionExpression = conditionExpression;
            return this;
        }

        /**
         * Sets a simple condition expression string.
         * 设置简单的条件表达式字符串。
         *
         * @param expression the condition expression string / 条件表达式字符串
         * @return this builder / 当前构建器
         */
        public ExpressionUpdateBuilder<T> condition(String expression) {
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
        public ExpressionUpdateBuilder<T> condition(String expression,
                                                     java.util.function.Consumer<ConditionExpression.Builder> configurator) {
            ConditionExpression.Builder builder = ConditionExpression.builder().expression(expression);
            configurator.accept(builder);
            this.conditionExpression = builder.build();
            return this;
        }

        // ---- Return values ----

        /**
         * Specifies what to return after the update.
         * 指定更新后返回的内容。
         *
         * @param returnValue the return value option / 返回值选项
         * @return this builder / 当前构建器
         */
        public ExpressionUpdateBuilder<T> returnValues(ReturnValue returnValue) {
            this.returnValue = returnValue;
            return this;
        }

        // ---- Execute ----

        /**
         * Executes the expression update.
         * 执行表达式更新。
         *
         * @return the raw UpdateItemResponse (contains old/new values if returnValues was set) / 原始 UpdateItemResponse（如果设置了 returnValues 则包含旧/新值）
         * @throws DynamoException if the update fails / 更新失败时抛出
         * @throws DynamoConditionFailedException if the condition expression evaluates to false / 条件表达式不满足时抛出
         */
        public UpdateItemResponse execute() {
            if (setClauses.isEmpty() && removeClauses.isEmpty()
                    && addClauses.isEmpty() && deleteClauses.isEmpty()) {
                throw new DynamoException("Expression update requires at least one SET, REMOVE, ADD, or DELETE clause");
            }

            Map<String, AttributeValue> keyMap = KeyBuilder.buildKeyMap(metadata, partitionKey, sortKey);

            // Build the update expression
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

            // Merge condition expression names/values
            Map<String, String> allNames = new HashMap<>(expressionNames);
            Map<String, AttributeValue> allValues = new HashMap<>(expressionValues);
            if (conditionExpression != null) {
                allNames.putAll(conditionExpression.getExpressionNames());
                allValues.putAll(conditionExpression.getExpressionValues());
            }

            log.debug("ExpressionUpdate table={}, expression={}, condition={}",
                    metadata.getTableName(), updateExpression,
                    conditionExpression != null ? conditionExpression.getExpression() : "none");

            UpdateItemRequest.Builder builder = UpdateItemRequest.builder()
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
            if (returnValue != null) {
                builder.returnValues(returnValue);
            }

            try {
                UpdateItemResponse response = dynamoDbClient.updateItem(builder.build());
                log.trace("ExpressionUpdate succeeded for table={}", metadata.getTableName());
                return response;
            } catch (ConditionalCheckFailedException e) {
                log.warn("ExpressionUpdate condition failed for table={}: {}",
                        metadata.getTableName(), e.getMessage());
                throw new DynamoConditionFailedException(
                        "Condition check failed for expression update on table "
                                + metadata.getTableName() + ": " + e.getMessage(), e);
            } catch (DynamoDbException e) {
                log.error("ExpressionUpdate failed for table={}: {}", metadata.getTableName(), e.getMessage());
                throw new DynamoException(
                        "Failed to execute expression update on table "
                                + metadata.getTableName() + ": " + e.getMessage(), e);
            }
        }
    }
}
