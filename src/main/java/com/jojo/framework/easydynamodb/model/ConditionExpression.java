package com.jojo.framework.easydynamodb.model;

import com.jojo.framework.easydynamodb.operation.AttributeValues;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a DynamoDB condition expression with its attribute names and values.
 * 表示一个 DynamoDB 条件表达式及其属性名称和属性值。
 * <p>
 * Used across save, update, delete, and transaction operations to specify
 * conditions that must be met for the operation to succeed.
 * 用于 save、update、delete 和事务操作中，指定操作成功所必须满足的条件。
 *
 * <pre>{@code
 * // Simple condition
 * ConditionExpression cond = ConditionExpression.of("attribute_not_exists(userId)");
 *
 * // Condition with values (auto-converted)
 * ConditionExpression cond = ConditionExpression.builder()
 *     .expression("#coins >= :required")
 *     .name("#coins", "coins")
 *     .value(":required", 100)
 *     .build();
 *
 * // Use in operations
 * ddm.save(user, c -> c.condition(cond));
 * ddm.update(user, u -> u.setCoins(newCoins), c -> c.condition(cond));
 * }</pre>
 */
public class ConditionExpression {

    private final String expression;
    private final Map<String, String> expressionNames;
    private final Map<String, AttributeValue> expressionValues;

    /**
     * Private constructor for creating a ConditionExpression.
     * 用于创建 ConditionExpression 的私有构造器。
     *
     * @param expression       the condition expression string / 条件表达式字符串
     * @param expressionNames  the expression attribute name mappings / 表达式属性名称映射
     * @param expressionValues the expression attribute value mappings / 表达式属性值映射
     */
    private ConditionExpression(String expression,
                                Map<String, String> expressionNames,
                                Map<String, AttributeValue> expressionValues) {
        this.expression = expression;
        this.expressionNames = expressionNames != null
                ? Collections.unmodifiableMap(expressionNames) : Collections.emptyMap();
        this.expressionValues = expressionValues != null
                ? Collections.unmodifiableMap(expressionValues) : Collections.emptyMap();
    }

    /**
     * Creates a simple condition expression without attribute names or values.
     * 创建一个不包含属性名称或属性值的简单条件表达式。
     *
     * @param expression the condition expression string / 条件表达式字符串
     * @return a new ConditionExpression / 新的 ConditionExpression 实例
     */
    public static ConditionExpression of(String expression) {
        return new ConditionExpression(expression, null, null);
    }

    /**
     * Creates a new builder for constructing a condition expression.
     * 创建一个用于构建条件表达式的新 Builder。
     *
     * @return a new Builder / 新的 Builder 实例
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the condition expression string.
     * 返回条件表达式字符串。
     *
     * @return the expression string / 表达式字符串
     */
    public String getExpression() {
        return expression;
    }

    /**
     * Returns the expression attribute name mappings.
     * 返回表达式属性名称映射。
     *
     * @return unmodifiable map of name placeholders to attribute names / 占位符到属性名称的不可变映射
     */
    public Map<String, String> getExpressionNames() {
        return expressionNames;
    }

    /**
     * Returns the expression attribute value mappings.
     * 返回表达式属性值映射。
     *
     * @return unmodifiable map of value placeholders to AttributeValues / 占位符到 AttributeValue 的不可变映射
     */
    public Map<String, AttributeValue> getExpressionValues() {
        return expressionValues;
    }

    /**
     * Fluent builder for constructing condition expressions.
     * 用于构建条件表达式的流式 Builder。
     */
    public static class Builder {
        private String expression;
        private Map<String, String> names;
        private Map<String, AttributeValue> values;

        /**
         * Private constructor for Builder.
         * Builder 的私有构造器。
         */
        private Builder() {}

        /**
         * Sets the condition expression string.
         * 设置条件表达式字符串。
         *
         * @param expression the condition expression (e.g. "#coins >= :required") / 条件表达式（例如 "#coins >= :required"）
         * @return this builder / 当前 Builder 实例
         */
        public Builder expression(String expression) {
            this.expression = expression;
            return this;
        }

        /**
         * Adds an expression attribute name mapping.
         * 添加一个表达式属性名称映射。
         *
         * @param placeholder   the placeholder (e.g. "#coins") / 占位符（例如 "#coins"）
         * @param attributeName the actual DynamoDB attribute name (e.g. "coins") / 实际的 DynamoDB 属性名称（例如 "coins"）
         * @return this builder / 当前 Builder 实例
         */
        public Builder name(String placeholder, String attributeName) {
            if (this.names == null) this.names = new HashMap<>();
            this.names.put(placeholder, attributeName);
            return this;
        }

        /**
         * Adds all expression attribute name mappings.
         * 添加所有表达式属性名称映射。
         *
         * @param names the name mappings / 名称映射
         * @return this builder / 当前 Builder 实例
         */
        public Builder names(Map<String, String> names) {
            if (this.names == null) this.names = new HashMap<>();
            this.names.putAll(names);
            return this;
        }

        /**
         * Adds an expression attribute value with auto-conversion from Java type.
         * 添加一个表达式属性值，自动从 Java 类型转换。
         *
         * @param placeholder the placeholder (e.g. ":required") / 占位符（例如 ":required"）
         * @param val         the Java value (String, Number, Boolean, Enum, etc.) / Java 值（String、Number、Boolean、Enum 等）
         * @return this builder / 当前 Builder 实例
         */
        public Builder value(String placeholder, Object val) {
            if (this.values == null) this.values = new HashMap<>();
            this.values.put(placeholder, AttributeValues.of(val));
            return this;
        }

        /**
         * Adds a raw AttributeValue expression value.
         * 添加一个原始的 AttributeValue 表达式值。
         *
         * @param placeholder    the placeholder / 占位符
         * @param attributeValue the raw AttributeValue / 原始的 AttributeValue
         * @return this builder / 当前 Builder 实例
         */
        public Builder rawValue(String placeholder, AttributeValue attributeValue) {
            if (this.values == null) this.values = new HashMap<>();
            this.values.put(placeholder, attributeValue);
            return this;
        }

        /**
         * Adds all expression attribute values (raw AttributeValue map).
         * 添加所有表达式属性值（原始 AttributeValue 映射）。
         *
         * @param values the value mappings / 值映射
         * @return this builder / 当前 Builder 实例
         */
        public Builder rawValues(Map<String, AttributeValue> values) {
            if (this.values == null) this.values = new HashMap<>();
            this.values.putAll(values);
            return this;
        }

        /**
         * Builds the ConditionExpression.
         * 构建 ConditionExpression 实例。
         *
         * @return a new ConditionExpression / 新的 ConditionExpression 实例
         * @throws IllegalStateException if expression is null or empty / 当表达式为 null 或空时抛出
         */
        public ConditionExpression build() {
            if (expression == null || expression.isEmpty()) {
                throw new IllegalStateException("Condition expression string must not be null or empty");
            }
            return new ConditionExpression(expression, names, values);
        }
    }
}
