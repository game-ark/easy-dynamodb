package com.jojo.framework.easydynamodb.operation;

import com.jojo.framework.easydynamodb.exception.DynamoException;
import com.jojo.framework.easydynamodb.logging.DdmLogger;
import com.jojo.framework.easydynamodb.metadata.EntityMetadata;
import com.jojo.framework.easydynamodb.metadata.MetadataRegistry;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Handles Scan operations against DynamoDB tables.
 * Supports filter expressions, limits, and automatic pagination.
 */
public class ScanOperation {

    private static final DdmLogger log = DdmLogger.getLogger(ScanOperation.class);

    private final DynamoDbClient dynamoDbClient;
    private final MetadataRegistry metadataRegistry;
    private final GetOperation getOperation;

    public ScanOperation(DynamoDbClient dynamoDbClient,
                         MetadataRegistry metadataRegistry,
                         GetOperation getOperation) {
        this.dynamoDbClient = dynamoDbClient;
        this.metadataRegistry = metadataRegistry;
        this.getOperation = getOperation;
    }

    public <T> ScanBuilder<T> scan(Class<T> clazz) {
        metadataRegistry.register(clazz);
        EntityMetadata metadata = metadataRegistry.getMetadata(clazz);
        return new ScanBuilder<>(clazz, metadata);
    }

    public class ScanBuilder<T> {
        private final Class<T> clazz;
        private final EntityMetadata metadata;
        private String filterExpression;
        private Map<String, AttributeValue> expressionValues;
        private Map<String, String> expressionNames;
        private Integer limit;
        private Map<String, AttributeValue> exclusiveStartKey;

        ScanBuilder(Class<T> clazz, EntityMetadata metadata) {
            this.clazz = clazz;
            this.metadata = metadata;
        }

        public ScanBuilder<T> filter(String expression) {
            this.filterExpression = expression;
            return this;
        }

        public ScanBuilder<T> expressionValues(Map<String, AttributeValue> values) {
            this.expressionValues = values;
            return this;
        }

        public ScanBuilder<T> expressionNames(Map<String, String> names) {
            this.expressionNames = names;
            return this;
        }

        public ScanBuilder<T> limit(int limit) {
            this.limit = limit;
            return this;
        }

        public ScanBuilder<T> startKey(Map<String, AttributeValue> startKey) {
            this.exclusiveStartKey = startKey;
            return this;
        }

        @SuppressWarnings("unchecked")
        public QueryOperation.QueryResult<T> execute() {
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

                return new QueryOperation.QueryResult<>(items, lastKey);
            } catch (DynamoDbException e) {
                log.error("Scan failed for table={}: {}", metadata.getTableName(), e.getMessage());
                throw new DynamoException(
                        "Failed to scan table " + metadata.getTableName() + ": " + e.getMessage(), e);
            }
        }

        @SuppressWarnings("unchecked")
        public List<T> executeAll() {
            List<T> allItems = new ArrayList<>();
            Map<String, AttributeValue> currentStartKey = this.exclusiveStartKey;
            int pageCount = 0;

            do {
                this.exclusiveStartKey = currentStartKey;
                QueryOperation.QueryResult<T> result = execute();
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
