package com.jojo.framework.easydynamodb.model;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.List;
import java.util.Map;

/**
 * Result of a paginated DynamoDB operation (Query or Scan),
 * containing the returned items and an optional pagination key.
 * 分页 DynamoDB 操作（Query 或 Scan）的结果，
 * 包含返回的条目和可选的分页键。
 *
 * @param items            the returned entities / 返回的实体列表
 * @param lastEvaluatedKey the last evaluated key for pagination (null if no more pages) / 分页的最后评估键（如果没有更多页则为 null）
 * @param <T>              the entity type / 实体类型
 */
public record PagedResult<T>(List<T> items, Map<String, AttributeValue> lastEvaluatedKey) {

    /**
     * Returns true if there are more pages to fetch.
     * 如果还有更多页面可获取，则返回 true。
     *
     * @return true if more pages exist / 如果存在更多页面则返回 true
     */
    public boolean hasMorePages() {
        return lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty();
    }
}
