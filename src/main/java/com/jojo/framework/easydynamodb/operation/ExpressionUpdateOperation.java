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
 * <p>
 * This complements {@link UpdateOperation} (which does entity-level diff-based updates)
 * by providing low-level expression control for scenarios like:
 * <ul>
 *   <li>Atomic counter increments: {@code SET coins = coins + :amount}</li>
 *   <li>Conditional updates: {@code SET status = :new IF status = :old}</li>
 *   <li>Adding to sets: {@code ADD tags :newTags}</li>
 *   <li>Removing attributes: {@code REMOVE tempFlag}</li>
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

    public ExpressionUpdateOperation(DynamoDbClient dynamoDbClient, MetadataRegistry metadataRegistry) {
        this.dynamoDbClient = dynamoDbClient;
        this.metadataRegistry = metadataRegistry;
    }

    /**
     * Creates a new ExpressionUpdateBuilder for the given entity class and primary key.
     *
     * @param clazz        the entity class
     * @param partitionKey the partition key value
     * @return a new builder
     */
    public <T> ExpressionUpdateBuilder<T> expressionUpdate(Class<T> clazz, Object partitionKey) {
        return expressionUpdate(clazz, partitionKey, null);
    }

    /**
     * Creates a new ExpressionUpdateBuilder for the given entity class and composite key.
     *
     * @param clazz        the entity class
     * @param partitionKey the partition key value
     * @param sortKey      the sort key value (nullable)
     * @return a new builder
     */
    public <T> ExpressionUpdateBuilder<T> expressionUpdate(Class<T> clazz, Object partitionKey, Object sortKey) {
        metadataRegistry.register(clazz);
        EntityMetadata metadata = metadataRegistry.getMetadata(clazz);
        return new ExpressionUpdateBuilder<>(clazz, metadata, partitionKey, sortKey);
    }

    /**
     * Fluent builder for constructing expression-based DynamoDB UpdateItem requests.
     * <p>
     * Supports all four DynamoDB update expression clauses:
     * <ul>
     *   <li>{@code SET} — assign values, including arithmetic expressions</li>
     *   <li>{@code REMOVE} — remove attributes from the item</li>
     *   <li>{@code ADD} — add to number or set attributes</li>
     *   <li>{@code DELETE} — remove elements from a set</li>
     * </ul>
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
         * <p>
         * Example: {@code .set("#coins = #coins + :amount")}
         *
         * @param setExpression the SET expression fragment
         * @return this builder
         */
        public ExpressionUpdateBuilder<T> set(String setExpression) {
            setClauses.add(setExpression);
            return this;
        }

        /**
         * Convenience: SET a single attribute to a value.
         * <p>
         * Example: {@code .set("status", "ACTIVE")} → {@code SET #f0 = :v0}
         *
         * @param attributeName the DynamoDB attribute name
         * @param value         the Java value (auto-converted)
         * @return this builder
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
         *
         * @param attributeName the DynamoDB attribute name
         * @param amount        the amount to add (positive to increment, negative to decrement)
         * @return this builder
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
         *
         * @param attributeName the DynamoDB attribute name
         * @param amount        the positive amount to subtract
         * @return this builder
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
         * <p>
         * Useful for initializing a counter that may not exist yet.
         *
         * @param attributeName the DynamoDB attribute name
         * @param defaultValue  the default value if attribute doesn't exist
         * @param addValue      the value to add
         * @return this builder
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
         *
         * @param attributeName the DynamoDB attribute name
         * @param value         the list value to append/prepend (as AttributeValue L type)
         * @param prepend       true to prepend, false to append
         * @return this builder
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
         * <p>
         * Example: {@code .remove("tempFlag", "oldField")}
         *
         * @param attributeNames the attribute names to remove
         * @return this builder
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
         *
         * @param removeExpression the REMOVE expression fragment
         * @return this builder
         */
        public ExpressionUpdateBuilder<T> removeRaw(String removeExpression) {
            removeClauses.add(removeExpression);
            return this;
        }

        // ---- ADD clause ----

        /**
         * Adds an ADD clause — adds a value to a number attribute or adds elements to a set.
         * <p>
         * Example: {@code .add("viewCount", 1)} or {@code .add("tags", setOfStrings)}
         *
         * @param attributeName the attribute name
         * @param value         the value to add
         * @return this builder
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
         *
         * @param addExpression the ADD expression fragment
         * @return this builder
         */
        public ExpressionUpdateBuilder<T> addRaw(String addExpression) {
            addClauses.add(addExpression);
            return this;
        }

        // ---- DELETE clause (set element removal) ----

        /**
         * Adds a DELETE clause — removes elements from a set attribute.
         *
         * @param attributeName the set attribute name
         * @param value         the set of elements to remove (as AttributeValue SS/NS/BS)
         * @return this builder
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
         *
         * @param placeholder   the placeholder (e.g. "#status")
         * @param attributeName the actual attribute name (e.g. "status")
         * @return this builder
         */
        public ExpressionUpdateBuilder<T> name(String placeholder, String attributeName) {
            expressionNames.put(placeholder, attributeName);
            return this;
        }

        /**
         * Adds an expression attribute value with auto-conversion.
         *
         * @param placeholder the placeholder (e.g. ":amount")
         * @param val         the Java value
         * @return this builder
         */
        public ExpressionUpdateBuilder<T> value(String placeholder, Object val) {
            expressionValues.put(placeholder, AttributeValues.of(val));
            return this;
        }

        /**
         * Adds a raw AttributeValue expression value.
         *
         * @param placeholder    the placeholder
         * @param attributeValue the raw AttributeValue
         * @return this builder
         */
        public ExpressionUpdateBuilder<T> rawValue(String placeholder, AttributeValue attributeValue) {
            expressionValues.put(placeholder, attributeValue);
            return this;
        }

        // ---- Condition expression ----

        /**
         * Sets a condition expression that must be satisfied for the update to proceed.
         *
         * @param conditionExpression the pre-built condition expression
         * @return this builder
         */
        public ExpressionUpdateBuilder<T> condition(ConditionExpression conditionExpression) {
            this.conditionExpression = conditionExpression;
            return this;
        }

        /**
         * Sets a simple condition expression string.
         *
         * @param expression the condition expression string
         * @return this builder
         */
        public ExpressionUpdateBuilder<T> condition(String expression) {
            this.conditionExpression = ConditionExpression.of(expression);
            return this;
        }

        /**
         * Sets a condition expression with a builder callback for names/values.
         *
         * @param expression the condition expression string
         * @param configurator callback to configure names and values
         * @return this builder
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
         *
         * @param returnValue the return value option
         * @return this builder
         */
        public ExpressionUpdateBuilder<T> returnValues(ReturnValue returnValue) {
            this.returnValue = returnValue;
            return this;
        }

        // ---- Execute ----

        /**
         * Executes the expression update.
         *
         * @return the raw UpdateItemResponse (contains old/new values if returnValues was set)
         * @throws DynamoException if the update fails
         * @throws DynamoConditionFailedException if the condition expression evaluates to false
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
