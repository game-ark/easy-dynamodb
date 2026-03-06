package com.jojo.framework.easydynamodb.model;

/**
 * 主键对，包含分区键和可选的排序键。
 * 用于批量获取操作中指定要获取的记录主键。
 *
 * @param partitionKey 分区键值
 * @param sortKey      排序键值，可为 null（当表没有排序键时）
 */
public record KeyPair(Object partitionKey, Object sortKey) {

    /**
     * 仅指定分区键的便捷构造器，sortKey 默认为 null。
     *
     * @param partitionKey 分区键值
     */
    public KeyPair(Object partitionKey) {
        this(partitionKey, null);
    }
}
