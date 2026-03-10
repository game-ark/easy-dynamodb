package com.jojo.framework.easydynamodb.model;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.List;
import java.util.Map;

/**
 * Result of a paginated DynamoDB operation (Query or Scan),
 * containing the returned items and an optional pagination key.
 *
 * @param items            the returned entities
 * @param lastEvaluatedKey the last evaluated key for pagination (null if no more pages)
 * @param <T>              the entity type
 */
public record PagedResult<T>(List<T> items, Map<String, AttributeValue> lastEvaluatedKey) {

    /**
     * Returns true if there are more pages to fetch.
     */
    public boolean hasMorePages() {
        return lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty();
    }
}
