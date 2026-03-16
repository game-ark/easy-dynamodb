package com.jojo.framework.easydynamodb.operation;

import com.jojo.framework.easydynamodb.exception.DynamoException;
import com.jojo.framework.easydynamodb.logging.DdmLogger;
import com.jojo.framework.easydynamodb.metadata.EntityMetadata;
import com.jojo.framework.easydynamodb.metadata.MetadataRegistry;
import com.jojo.framework.easydynamodb.model.PagedResult;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handles Scan operations against DynamoDB tables.
 * Supports filter expressions, limits, and automatic pagination.
 * 处理针对 DynamoDB 表的扫描操作。
 * 支持过滤表达式、限制和自动分页。
 */
public class ScanOperation {

    private static final DdmLogger log = DdmLogger.getLogger(ScanOperation.class);

    private final DynamoDbClient dynamoDbClient;
    private final MetadataRegistry metadataRegistry;
    private final GetOperation getOperation;

    /**
     * Constructs a ScanOperation.
     * 构造 ScanOperation。
     *
     * @param dynamoDbClient   the DynamoDB client / DynamoDB 客户端
     * @param metadataRegistry the metadata registry / 元数据注册中心
     * @param getOperation     the get operation for entity conversion / 用于实体转换的获取操作
     */
    public ScanOperation(DynamoDbClient dynamoDbClient,
                         MetadataRegistry metadataRegistry,
                         GetOperation getOperation) {
        this.dynamoDbClient = dynamoDbClient;
        this.metadataRegistry = metadataRegistry;
        this.getOperation = getOperation;
    }

    /**
     * Creates a new ScanBuilder for the given entity class.
     * 为给定的实体类创建新的 ScanBuilder。
     *
     * @param clazz the entity class / 实体类
     * @param <T>   the entity type / 实体类型
     * @return a new ScanBuilder instance / 新的 ScanBuilder 实例
     */
    public <T> ScanBuilder<T> scan(Class<T> clazz) {
        metadataRegistry.register(clazz);
        EntityMetadata metadata = metadataRegistry.getMetadata(clazz);
        return new ScanBuilder<>(clazz, metadata);
    }

    /**
     * Fluent builder for constructing and executing DynamoDB Scan requests.
     * 用于构建和执行 DynamoDB Scan 请求的流式构建器。
     *
     * @param <T> the entity type / 实体类型
     */
    public class ScanBuilder<T> {
        private final Class<T> clazz;
        private final EntityMetadata metadata;
        private String filterExpression;
        private Map<String, AttributeValue> expressionValues;
        private Map<String, String> expressionNames;
        private Integer limit;
        private Boolean consistentRead;
        private String projectionExpression;
        private Map<String, AttributeValue> exclusiveStartKey;

        ScanBuilder(Class<T> clazz, EntityMetadata metadata) {
            this.clazz = clazz;
            this.metadata = metadata;
        }

        /**
         * Set a filter expression applied during the scan.
         * 设置扫描期间应用的过滤表达式。
         *
         * @param expression the filter expression / 过滤表达式
         * @return this builder / 当前构建器
         */
        public ScanBuilder<T> filter(String expression) {
            this.filterExpression = expression;
            return this;
        }

        /**
         * Set expression attribute values (e.g. ":min" → AttributeValue).
         * 设置表达式属性值（例如 ":min" → AttributeValue）。
         *
         * @param values the expression attribute values map / 表达式属性值映射
         * @return this builder / 当前构建器
         */
        public ScanBuilder<T> expressionValues(Map<String, AttributeValue> values) {
            this.expressionValues = values != null ? new HashMap<>(values) : null;
            return this;
        }

        /**
         * Shorthand for adding a single expression attribute value.
         * Automatically converts the Java value to AttributeValue.
         * 添加单个表达式属性值的简写方法。自动将 Java 值转换为 AttributeValue。
         * <p>
         * Example: {@code .value(":min", 9.0)}
         *
         * @param placeholder the expression placeholder (e.g. ":min") / 表达式占位符（例如 ":min"）
         * @param val         the Java value (String, Number, Boolean, Enum, Instant, etc.) / Java 值（String、Number、Boolean、Enum、Instant 等）
         * @return this builder / 当前构建器
         */
        public ScanBuilder<T> value(String placeholder, Object val) {
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
        public ScanBuilder<T> expressionNames(Map<String, String> names) {
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
        public ScanBuilder<T> limit(int limit) {
            this.limit = limit;
            return this;
        }

        /**
         * Enable or disable consistent read. Default is eventually consistent (false).
         * 启用或禁用强一致性读取。默认为最终一致性（false）。
         *
         * @param consistentRead true for strongly consistent read / true 为强一致性读取
         * @return this builder / 当前构建器
         */
        public ScanBuilder<T> consistentRead(boolean consistentRead) {
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
        public ScanBuilder<T> projection(String projectionExpression) {
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
        public ScanBuilder<T> startKey(Map<String, AttributeValue> startKey) {
            this.exclusiveStartKey = startKey;
            return this;
        }

        /**
         * Execute the scan and return a single page of results.
         * 执行扫描并返回单页结果。
         *
         * @return the paged result containing items and optional pagination key / 包含项和可选分页键的分页结果
         * @throws DynamoException if the scan fails / 扫描失败时抛出
         */
        @SuppressWarnings("unchecked")
        public PagedResult<T> execute() {
            log.debug("Scan table={}, filter={}, limit={}",
                    metadata.getTableName(), filterExpression, limit);

            ScanRequest.Builder builder = ScanRequest.builder()
                    .tableName(metadata.getTableName());

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
                ScanResponse response = dynamoDbClient.scan(builder.build());

                List<T> items = new ArrayList<>();
                if (response.hasItems()) {
                    for (Map<String, AttributeValue> item : response.items()) {
                        items.add((T) getOperation.fromAttributeValueMap(item, metadata));
                    }
                }

                Map<String, AttributeValue> lastKey = response.hasLastEvaluatedKey()
                        ? response.lastEvaluatedKey() : null;

                log.debug("Scan table={} returned {} items, hasMore={}", metadata.getTableName(),
                        items.size(), lastKey != null);

                return new PagedResult<>(items, lastKey);
            } catch (DynamoDbException e) {
                log.error("Scan failed for table={}: {}", metadata.getTableName(), e.getMessage());
                throw new DynamoException(
                        "Failed to scan table " + metadata.getTableName() + ": " + e.getMessage(), e);
            }
        }

        /**
         * Execute the scan and automatically paginate to collect all results.
         * 执行扫描并自动分页以收集所有结果。
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
                PagedResult<T> result = execute();
                allItems.addAll(result.items());
                currentStartKey = result.lastEvaluatedKey();
                pageCount++;
            } while (currentStartKey != null && !currentStartKey.isEmpty());

            log.info("Scan executeAll on table={} completed: {} pages, {} total items",
                    metadata.getTableName(), pageCount, allItems.size());

            return allItems;
        }
    }
}
