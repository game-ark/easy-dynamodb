package com.jojo.framework.easydynamodb.operation;

import com.jojo.framework.easydynamodb.exception.DynamoException;
import com.jojo.framework.easydynamodb.logging.DdmLogger;
import com.jojo.framework.easydynamodb.metadata.EntityMetadata;
import com.jojo.framework.easydynamodb.metadata.MetadataRegistry;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Handles Query operations against DynamoDB tables and GSIs.
 * Supports key conditions, filter expressions, limits, ascending/descending order,
 * and automatic pagination.
 */
public class QueryOperation {

    private static final DdmLogger log = DdmLogger.getLogger(QueryOperation.class);

    private final DynamoDbClient dynamoDbClient;
    private final MetadataRegistry metadataRegistry;
    private final GetOperation getOperation;

    public QueryOperation(DynamoDbClient dynamoDbClient,
                          MetadataRegistry metadataRegistry,
                          GetOperation getOperation) {
        this.dynamoDbClient = dynamoDbClient;
        this.metadataRegistry = metadataRegistry;
        this.getOperation = getOperation;
    }

    /**
     * Creates a new QueryBuilder for the given entity class.
     */
    public <T> QueryBuilder<T> query(Class<T> clazz) {
        metadataRegistry.register(clazz);
        EntityMetadata metadata = metadataRegistry.getMetadata(clazz);
        return new QueryBuilder<>(clazz, metadata);
    }

    /**
     * Fluent builder for constructing and executing DynamoDB Query requests.
     */
    public class QueryBuilder<T> {
        private final Class<T> clazz;
        private final EntityMetadata metadata;
        private String indexName;
        private String keyConditionExpression;
        private String filterExpression;
        private Map<String, AttributeValue> expressionValues;
        private Map<String, String> expressionNames;
        private Integer limit;
        private Boolean scanForward = true;
        private Boolean consistentRead;
        private String projectionExpression;
        private Map<String, AttributeValue> exclusiveStartKey;

        QueryBuilder(Class<T> clazz, EntityMetadata metadata) {
            this.clazz = clazz;
            this.metadata = metadata;
        }

        /** Use a GSI index for this query. */
        public QueryBuilder<T> index(String indexName) {
            this.indexName = indexName;
            return this;
        }

        /** Set the key condition expression (e.g. "pk = :pk AND sk BEGINS_WITH :prefix"). */
        public QueryBuilder<T> keyCondition(String expression) {
            this.keyConditionExpression = expression;
            return this;
        }

        /** Set a filter expression applied after the query. */
        public QueryBuilder<T> filter(String expression) {
            this.filterExpression = expression;
            return this;
        }

        /** Set expression attribute values (e.g. ":pk" → AttributeValue). */
        public QueryBuilder<T> expressionValues(Map<String, AttributeValue> values) {
            this.expressionValues = values != null ? new java.util.HashMap<>(values) : null;
            return this;
        }

        /**
         * Shorthand for adding a single expression attribute value.
         * Automatically converts the Java value to AttributeValue.
         * <p>
         * Example: {@code .value(":genre", "RPG").value(":min", 9.0)}
         *
         * @param placeholder the expression placeholder (e.g. ":genre")
         * @param val         the Java value (String, Number, Boolean, Enum, Instant, etc.)
         * @return this builder
         */
        public QueryBuilder<T> value(String placeholder, Object val) {
            if (this.expressionValues == null) {
                this.expressionValues = new java.util.HashMap<>();
            }
            this.expressionValues.put(placeholder, AttributeValues.of(val));
            return this;
        }

        /** Set expression attribute names (e.g. "#s" → "status"). */
        public QueryBuilder<T> expressionNames(Map<String, String> names) {
            this.expressionNames = names;
            return this;
        }

        /** Limit the number of items evaluated. */
        public QueryBuilder<T> limit(int limit) {
            this.limit = limit;
            return this;
        }

        /** Sort ascending (default). */
        public QueryBuilder<T> ascending() {
            this.scanForward = true;
            return this;
        }

        /** Sort descending. */
        public QueryBuilder<T> descending() {
            this.scanForward = false;
            return this;
        }

        /**
         * Enable or disable consistent read. Default is eventually consistent (false).
         * Note: ConsistentRead is not supported on GSI queries.
         *
         * @param consistentRead true for strongly consistent read
         * @return this builder
         */
        public QueryBuilder<T> consistentRead(boolean consistentRead) {
            this.consistentRead = consistentRead;
            return this;
        }

        /**
         * Set a projection expression to return only specific attributes.
         * <p>
         * Example: {@code .projection("gameId, title, rating")}
         *
         * @param projectionExpression the projection expression
         * @return this builder
         */
        public QueryBuilder<T> projection(String projectionExpression) {
            this.projectionExpression = projectionExpression;
            return this;
        }

        /** Set the exclusive start key for pagination. */
        public QueryBuilder<T> startKey(Map<String, AttributeValue> startKey) {
            this.exclusiveStartKey = startKey;
            return this;
        }

        /** Execute the query and return results. */
        @SuppressWarnings("unchecked")
        public QueryResult<T> execute() {
            if (keyConditionExpression == null || keyConditionExpression.isEmpty()) {
                throw new DynamoException("keyCondition is required for query operations");
            }

            log.debug("Query table={}, index={}, keyCondition={}, filter={}, limit={}, scanForward={}",
                    metadata.getTableName(), indexName, keyConditionExpression,
                    filterExpression, limit, scanForward);

            QueryRequest.Builder builder = QueryRequest.builder()
                    .tableName(metadata.getTableName())
                    .keyConditionExpression(keyConditionExpression)
                    .scanIndexForward(scanForward);

            if (indexName != null) {
                builder.indexName(indexName);
            }
            if (filterExpression != null) {
                builder.filterExpression(filterExpression);
            }
            if (expressionValues != null && !expressionValues.isEmpty()) {
                builder.expressionAttributeValues(expressionValues);
            }
            if (expressionNames != null && !expressionNames.isEmpty()) {
                builder.expressionAttributeNames(expressionNames);
            }
            if (limit != null) {
                builder.limit(limit);
            }
            if (consistentRead != null) {
                builder.consistentRead(consistentRead);
            }
            if (projectionExpression != null) {
                builder.projectionExpression(projectionExpression);
            }
            if (exclusiveStartKey != null && !exclusiveStartKey.isEmpty()) {
                builder.exclusiveStartKey(exclusiveStartKey);
            }

            try {
                QueryResponse response = dynamoDbClient.query(builder.build());

                List<T> items = new ArrayList<>();
                if (response.hasItems()) {
                    for (Map<String, AttributeValue> item : response.items()) {
                        items.add((T) getOperation.fromAttributeValueMap(item, metadata));
                    }
                }

                Map<String, AttributeValue> lastKey = response.hasLastEvaluatedKey()
                        ? response.lastEvaluatedKey() : null;

                log.debug("Query table={} returned {} items, hasMore={}", metadata.getTableName(),
                        items.size(), lastKey != null);

                return new QueryResult<>(items, lastKey);
            } catch (DynamoDbException e) {
                log.error("Query failed for table={}: {}", metadata.getTableName(), e.getMessage());
                throw new DynamoException(
                        "Failed to query table " + metadata.getTableName()
                                + (indexName != null ? " (index: " + indexName + ")" : "")
                                + ": " + e.getMessage(), e);
            }
        }

        /** Execute the query and automatically paginate to collect all results. */
        @SuppressWarnings("unchecked")
        public List<T> executeAll() {
            List<T> allItems = new ArrayList<>();
            Map<String, AttributeValue> currentStartKey = this.exclusiveStartKey;
            int pageCount = 0;

            do {
                this.exclusiveStartKey = currentStartKey;
                QueryResult<T> result = execute();
                allItems.addAll(result.items());
                currentStartKey = result.lastEvaluatedKey();
                pageCount++;
            } while (currentStartKey != null && !currentStartKey.isEmpty());

            log.info("Query executeAll on table={} completed: {} pages, {} total items",
                    metadata.getTableName(), pageCount, allItems.size());

            return allItems;
        }
    }

    /**
     * Result of a query operation, containing items and optional pagination key.
     *
     * @param items            the returned entities
     * @param lastEvaluatedKey the last evaluated key for pagination (null if no more pages)
     */
    public record QueryResult<T>(List<T> items, Map<String, AttributeValue> lastEvaluatedKey) {
        public boolean hasMorePages() {
            return lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty();
        }

        /** Converts to the new PagedResult type. */
        public com.jojo.framework.easydynamodb.model.PagedResult<T> toPagedResult() {
            return new com.jojo.framework.easydynamodb.model.PagedResult<>(items, lastEvaluatedKey);
        }
    }
}
