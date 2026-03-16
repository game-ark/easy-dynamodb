package com.jojo.framework.easydynamodb.metadata;

/**
 * Metadata for a Global Secondary Index (GSI), containing the index name,
 * partition key, and optional sort key.
 * 全局二级索引（GSI）的元数据，包含索引名称、分区键和可选的排序键。
 */
public class GsiMetadata {

    private final String indexName;
    private final FieldMetadata partitionKey;
    private final FieldMetadata sortKey; // nullable

    /**
     * Constructs a GsiMetadata instance with the given index name, partition key, and sort key.
     * 使用给定的索引名称、分区键和排序键构造 GsiMetadata 实例。
     *
     * @param indexName    the GSI index name / GSI 索引名称
     * @param partitionKey the partition key field metadata / 分区键字段元数据
     * @param sortKey      the sort key field metadata, may be null / 排序键字段元数据，可为 null
     */
    public GsiMetadata(String indexName, FieldMetadata partitionKey, FieldMetadata sortKey) {
        this.indexName = indexName;
        this.partitionKey = partitionKey;
        this.sortKey = sortKey;
    }

    /**
     * Returns the GSI index name.
     * 返回 GSI 索引名称。
     *
     * @return the index name / 索引名称
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * Returns the partition key field metadata for this GSI.
     * 返回此 GSI 的分区键字段元数据。
     *
     * @return the partition key metadata / 分区键元数据
     */
    public FieldMetadata getPartitionKey() {
        return partitionKey;
    }

    /**
     * Returns the sort key field metadata for this GSI, or null if not defined.
     * 返回此 GSI 的排序键字段元数据，如果未定义则返回 null。
     *
     * @return the sort key metadata, or null / 排序键元数据，或 null
     */
    public FieldMetadata getSortKey() {
        return sortKey;
    }
}
