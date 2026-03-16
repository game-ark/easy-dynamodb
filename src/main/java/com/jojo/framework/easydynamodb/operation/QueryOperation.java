package com.jojo.framework.easydynamodb.operation;

import com.jojo.framework.easydynamodb.exception.DynamoException;
import com.jojo.framework.easydynamodb.logging.DdmLogger;
import com.jojo.framework.easydynamodb.metadata.EntityMetadata;
import com.jojo.framework.easydynamodb.metadata.MetadataRegistry;
import com.jojo.framework.easydynamodb.model.PagedResult;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handles Query operations against DynamoDB tables and GSIs.
 * Supports key conditions, filter expressions, limits, ascending/descending order,
 * and automatic pagination.
 * 处理针对 DynamoDB 表和 GSI 的查询操作。
 * 支持键条件、过滤表达式、限制、升序/降序排列以及自动分页。
 */
public class QueryOperation {

    private static final DdmLogger log = DdmLogger.getLogger(QueryOperation.class);

    private final DynamoDbClient dynamoDbClient;
    private final MetadataRegistry metadataRegistry;
    private final GetOperation getOperation;

    /**
     * Constructs a QueryOperation.
     * 构造 QueryOperation。
     *
     * @param dynamoDbClient   the DynamoDB client / DynamoDB 客户端
     * @param metadataRegistry the metadata registry / 元数据注册中心
     * @param getOperation     the get operation for entity conversion / 用于实体转换的获取操作
     */
    public QueryOperation(DynamoDbClient dynamoDbClient,
                          MetadataRegistry metadataRegistry,
                          GetOperation getOperation) {
        this.dynamoDbClient = dynamoDbClient;
        this.metadataRegistry = metadataRegistry;
        this.getOperation = getOperation;
    }

    /**
     * Creates a new QueryBuilder for the given entity class.
     * 为给定的实体类创建新的 QueryBuilder。
     *
     * @param clazz the entity class / 实体类
     * @param <T>   the entity type / 实体类型
     * @return a new QueryBuilder instance / 新的 QueryBuilder 实例
     */
    public <T> QueryBuilder<T> query(Class<T> clazz) {
        metadataRegistry.register(clazz);
        EntityMetadata metadata = metadataRegistry.getMetadata(clazz);
        return new QueryBuilder<>(clazz, metadata);
    }

    /**
     * Fluent builder for constructing and executing DynamoDB Query requests.
     * 用于构建和执行 DynamoDB Query 请求的流式构建器。
     *
     * @param <T> the entity type / 实体类型
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

        /**
         * Use a GSI index for this query.
         * 使用 GSI 索引进行此查询。
         *
         * @param indexName the GSI index name / GSI 索引名称
         * @return this builder / 当前构建器
         */
        public QueryBuilder<T> index(String indexName) {
            this.indexName = indexName;
            return this;
        }

        /**
         * Set the key condition expression (e.g. "pk = :pk AND sk BEGINS_WITH :prefix").
         * 设置键条件表达式（例如 "pk = :pk AND sk BEGINS_WITH :prefix"）。
         *
         * @param expression the key condition expression / 键条件表达式
         * @return this builder / 当前构建器
         */
        public QueryBuilder<T> keyCondition(String expression) {
            this.keyConditionExpression = expression;
            return this;
        }

        /**
         * Set a filter expression applied after the query.
         * 设置查询后应用的过滤表达式。
         *
         * @param expression the filter expression / 过滤表达式
         * @return this builder / 当前构建器
         */
        public QueryBuilder<T> filter(String expression) {
            this.filterExpression = expression;
            return this;
        }

        /**
         * Set expression attribute values (e.g. ":pk" → AttributeValue).
         * 设置表达式属性值（例如 ":pk" → AttributeValue）。
         *
         * @param values the expression attribute values map / 表达式属性值映射
         * @return this builder / 当前构建器
         */
        public QueryBuilder<T> expressionValues(Map<String, AttributeValue> values) {
            this.expressionValues = values != null ? new HashMap<>(values) : null;
            return this;
        }

        /**
         * Shorthand for adding a single expression attribute value.
         * Automatically converts the Java value to AttributeValue.
         * 添加单个表达式属性值的简写方法。自动将 Java 值转换为 AttributeValue。
         * <p>
         * Example: {@code .value(":genre", "RPG").value(":min", 9.0)}
         *
         * @param placeholder the expression placeholder (e.g. ":genre") / 表达式占位符（例如 ":genre"）
         * @param val         the Java value (String, Number, Boolean, Enum, Instant, etc.) / Java 值（String、Number、Boolean、Enum、Instant 等）
         * @return this builder / 当前构建器
         */
        public QueryBuilder<T> value(String placeholder, Object val) {
            if (this.expressionValues == null) {
                this.expressionValues = new HashMap<>();
            }
            this.expressionValues.put(placeholder, AttributeValues.of(val));
            return this;
        }

        /**
         * Set expression attribute names (e.g. "#s" → "status").
         * 设置表达式属性名（例如 "#s" → "status"）。
         *
         * @param names the expression attribute names map / 表达式属性名映射
         * @return this builder / 当前构建器
         */
        public QueryBuilder<T> expressionNames(Map<String, String> names) {
            this.expressionNames = names;
            return this;
        }

        /**
         * Limit the number of items evaluated.
         * 限制评估的项数。
         *
         * @param limit the maximum number of items to evaluate / 最大评估项数
         * @return this builder / 当前构建器
         */
        public QueryBuilder<T> limit(int limit) {
            this.limit = limit;
            return this;
        }

        /**
         * Sort ascending (default).
         * 升序排列（默认）。
         *
         * @return this builder / 当前构建器
         */
        public QueryBuilder<T> ascending() {
            this.scanForward = true;
            return this;
        }

        /**
         * Sort descending.
         * 降序排列。
         *
         * @return this builder / 当前构建器
         */
        public QueryBuilder<T> descending() {
            this.scanForward = false;
            return this;
        }

        /**
         * Enable or disable consistent read. Default is eventually consistent (false).
         * Note: ConsistentRead is not supported on GSI queries.
         * 启用或禁用强一致性读取。默认为最终一致性（false）。
         * 注意：GSI 查询不支持 ConsistentRead。
         *
         * @param consistentRead true for strongly consistent read / true 为强一致性读取
         * @return this builder / 当前构建器
         */
        public QueryBuilder<T> consistentRead(boolean consistentRead) {
            this.consistentRead = consistentRead;
            return this;
        }

        /**
         * Set a projection expression to return only specific attributes.
         * 设置投影表达式以仅返回特定属性。
         * <p>
         * Example: {@code .projection("gameId, title, rating")}
         *
         * @param projectionExpression the projection expression / 投影表达式
         * @return this builder / 当前构建器
         */
        public QueryBuilder<T> projection(String projectionExpression) {
            this.projectionExpression = projectionExpression;
            return this;
        }

        /**
         * Set the exclusive start key for pagination.
         * 设置分页的排他起始键。
         *
         * @param startKey the exclusive start key map / 排他起始键映射
         * @return this builder / 当前构建器
         */
        public QueryBuilder<T> startKey(Map<String, AttributeValue> startKey) {
            this.exclusiveStartKey = startKey;
            return this;
        }

        /**
         * Execute the query and return results.
         * 执行查询并返回结果。
         *
         * @return the query result containing items and optional pagination key / 包含项和可选分页键的查询结果
         * @throws DynamoException if the query fails / 查询失败时抛出
         */
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

        /**
         * Execute the query and automatically paginate to collect all results.
         * 执行查询并自动分页以收集所有结果。
         *
         * @return all items across all pages / 所有分页的全部项
         */
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
     * 查询操作的结果，包含项和可选的分页键。
     *
     * @param items            the returned entities / 返回的实体列表
     * @param lastEvaluatedKey the last evaluated key for pagination (null if no more pages) / 分页的最后评估键（无更多页时为 null）
     * @param <T>              the entity type / 实体类型
     */
    public record QueryResult<T>(List<T> items, Map<String, AttributeValue> lastEvaluatedKey) {
        /**
         * Checks if there are more pages available.
         * 检查是否还有更多分页可用。
         *
         * @return true if more pages exist / 如果存在更多分页则返回 true
         */
        public boolean hasMorePages() {
            return lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty();
        }

        /**
         * Converts to PagedResult type.
         * 转换为 PagedResult 类型。
         *
         * @return the paged result / 分页结果
         */
        public PagedResult<T> toPagedResult() {
            return new PagedResult<>(items, lastEvaluatedKey);
        }
    }
}
