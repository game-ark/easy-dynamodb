package com.jojo.framework.easydynamodb.converter;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Interface for converting between Java types and DynamoDB AttributeValue.
 * 用于在 Java 类型与 DynamoDB AttributeValue 之间进行转换的接口。
 *
 * @param <T> the Java type this converter handles / 此转换器处理的 Java 类型
 */
public interface AttributeConverter<T> {

    /**
     * Converts a Java value to a DynamoDB AttributeValue.
     * 将 Java 值转换为 DynamoDB AttributeValue。
     *
     * @param value the Java value to convert / 要转换的 Java 值
     * @return the corresponding DynamoDB AttributeValue / 对应的 DynamoDB AttributeValue
     */
    AttributeValue toAttributeValue(T value);

    /**
     * Converts a DynamoDB AttributeValue back to a Java value.
     * 将 DynamoDB AttributeValue 转换回 Java 值。
     *
     * @param attributeValue the DynamoDB AttributeValue to convert / 要转换的 DynamoDB AttributeValue
     * @return the corresponding Java value / 对应的 Java 值
     */
    T fromAttributeValue(AttributeValue attributeValue);

    /**
     * Returns the Java class that this converter handles.
     * 返回此转换器处理的 Java 类。
     *
     * @return the target Java class / 目标 Java 类
     */
    Class<T> targetType();
}
