package com.jojo.framework.easydynamodb.model;

import com.jojo.framework.easydynamodb.operation.AttributeValues;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a DynamoDB condition expression with its attribute names and values.
 * <p>
 * Used across save, update, delete, and transaction operations to specify
 * conditions that must be met for the operation to succeed.
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
     *
     * @param expression the condition expression string
     * @return a new ConditionExpression
     */
    public static ConditionExpression of(String expression) {
        return new ConditionExpression(expression, null, null);
    }

    /**
     * Creates a new builder for constructing a condition expression.
     *
     * @return a new Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    public String getExpression() {
        return expression;
    }

    public Map<String, String> getExpressionNames() {
        return expressionNames;
    }

    public Map<String, AttributeValue> getExpressionValues() {
        return expressionValues;
    }

    /**
     * Fluent builder for constructing condition expressions.
     */
    public static class Builder {
        private String expression;
        private Map<String, String> names;
        private Map<String, AttributeValue> values;

        private Builder() {}

        /**
         * Sets the condition expression string.
         *
         * @param expression the condition expression (e.g. "#coins >= :required")
         * @return this builder
         */
        public Builder expression(String expression) {
            this.expression = expression;
            return this;
        }

        /**
         * Adds an expression attribute name mapping.
         *
         * @param placeholder the placeholder (e.g. "#coins")
         * @param attributeName the actual DynamoDB attribute name (e.g. "coins")
         * @return this builder
         */
        public Builder name(String placeholder, String attributeName) {
            if (this.names == null) this.names = new HashMap<>();
            this.names.put(placeholder, attributeName);
            return this;
        }

        /**
         * Adds all expression attribute name mappings.
         *
         * @param names the name mappings
         * @return this builder
         */
        public Builder names(Map<String, String> names) {
            if (this.names == null) this.names = new HashMap<>();
            this.names.putAll(names);
            return this;
        }

        /**
         * Adds an expression attribute value with auto-conversion from Java type.
         *
         * @param placeholder the placeholder (e.g. ":required")
         * @param val the Java value (String, Number, Boolean, Enum, etc.)
         * @return this builder
         */
        public Builder value(String placeholder, Object val) {
            if (this.values == null) this.values = new HashMap<>();
            this.values.put(placeholder, AttributeValues.of(val));
            return this;
        }

        /**
         * Adds a raw AttributeValue expression value.
         *
         * @param placeholder the placeholder
         * @param attributeValue the raw AttributeValue
         * @return this builder
         */
        public Builder rawValue(String placeholder, AttributeValue attributeValue) {
            if (this.values == null) this.values = new HashMap<>();
            this.values.put(placeholder, attributeValue);
            return this;
        }

        /**
         * Adds all expression attribute values (raw AttributeValue map).
         *
         * @param values the value mappings
         * @return this builder
         */
        public Builder rawValues(Map<String, AttributeValue> values) {
            if (this.values == null) this.values = new HashMap<>();
            this.values.putAll(values);
            return this;
        }

        /**
         * Builds the ConditionExpression.
         *
         * @return a new ConditionExpression
         * @throws IllegalStateException if expression is null or empty
         */
        public ConditionExpression build() {
            if (expression == null || expression.isEmpty()) {
                throw new IllegalStateException("Condition expression string must not be null or empty");
            }
            return new ConditionExpression(expression, names, values);
        }
    }
}
