package com.jojo.framework.easydynamodb.model;

/**
 * A key pair containing a partition key and an optional sort key.
 * 主键对，包含分区键和可选的排序键。
 * <p>
 * Used in batch get operations to specify the primary keys of records to retrieve.
 * 用于批量获取操作中指定要获取的记录主键。
 *
 * @param partitionKey the partition key value / 分区键值
 * @param sortKey      the sort key value, may be null if the table has no sort key / 排序键值，可为 null（当表没有排序键时）
 */
public record KeyPair(Object partitionKey, Object sortKey) {

    /**
     * Convenience constructor with only a partition key; sortKey defaults to null.
     * 仅指定分区键的便捷构造器，sortKey 默认为 null。
     *
     * @param partitionKey the partition key value / 分区键值
     */
    public KeyPair(Object partitionKey) {
        this(partitionKey, null);
    }
}
