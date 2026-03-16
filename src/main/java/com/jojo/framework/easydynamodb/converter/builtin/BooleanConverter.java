package com.jojo.framework.easydynamodb.converter.builtin;

import com.jojo.framework.easydynamodb.converter.AttributeConverter;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Converts Boolean ↔ DynamoDB BOOL type.
 * 转换 Boolean ↔ DynamoDB BOOL 类型。
 */
public class BooleanConverter implements AttributeConverter<Boolean> {

    /**
     * Converts a Boolean value to a DynamoDB BOOL AttributeValue.
     * 将 Boolean 值转换为 DynamoDB BOOL AttributeValue。
     *
     * @param value the Boolean value to convert / 要转换的 Boolean 值
     * @return the DynamoDB BOOL AttributeValue / DynamoDB BOOL 类型的 AttributeValue
     */
    @Override
    public AttributeValue toAttributeValue(Boolean value) {
        return AttributeValue.builder().bool(value).build();
    }

    /**
     * Converts a DynamoDB BOOL AttributeValue back to a Boolean.
     * 将 DynamoDB BOOL AttributeValue 转换回 Boolean。
     *
     * @param attributeValue the DynamoDB AttributeValue to convert / 要转换的 DynamoDB AttributeValue
     * @return the extracted Boolean value / 提取的 Boolean 值
     */
    @Override
    public Boolean fromAttributeValue(AttributeValue attributeValue) {
        return attributeValue.bool();
    }

    /**
     * Returns the target type this converter handles: {@code Boolean.class}.
     * 返回此转换器处理的目标类型：{@code Boolean.class}。
     *
     * @return {@code Boolean.class} / {@code Boolean.class}
     */
    @Override
    public Class<Boolean> targetType() {
        return Boolean.class;
    }
}
