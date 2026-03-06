package com.jojo.framework.easydynamodb.metadata;

/**
 * 全局二级索引（GSI）的元数据，包含索引名称、分区键和可选的排序键。
 */
public class GsiMetadata {

    private final String indexName;
    private final FieldMetadata partitionKey;
    private final FieldMetadata sortKey; // nullable

    public GsiMetadata(String indexName, FieldMetadata partitionKey, FieldMetadata sortKey) {
        this.indexName = indexName;
        this.partitionKey = partitionKey;
        this.sortKey = sortKey;
    }

    public String getIndexName() {
        return indexName;
    }

    public FieldMetadata getPartitionKey() {
        return partitionKey;
    }

    public FieldMetadata getSortKey() {
        return sortKey;
    }
}
