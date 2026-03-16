package com.jojo.framework.easydynamodb.converter.builtin;

import com.jojo.framework.easydynamodb.converter.AttributeConverter;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Converts String ↔ DynamoDB S (String) type.
 * 转换 String ↔ DynamoDB S（字符串）类型。
 */
public class StringConverter implements AttributeConverter<String> {

    /**
     * Converts a String value to a DynamoDB S (String) AttributeValue.
     * 将 String 值转换为 DynamoDB S（字符串）AttributeValue。
     *
     * @param value the String value to convert / 要转换的 String 值
     * @return the DynamoDB S AttributeValue / DynamoDB S 类型的 AttributeValue
     */
    @Override
    public AttributeValue toAttributeValue(String value) {
        return AttributeValue.builder().s(value).build();
    }

    /**
     * Converts a DynamoDB S (String) AttributeValue back to a String.
     * 将 DynamoDB S（字符串）AttributeValue 转换回 String。
     *
     * @param attributeValue the DynamoDB AttributeValue to convert / 要转换的 DynamoDB AttributeValue
     * @return the extracted String value / 提取的 String 值
     */
    @Override
    public String fromAttributeValue(AttributeValue attributeValue) {
        return attributeValue.s();
    }

    /**
     * Returns the target type this converter handles: {@code String.class}.
     * 返回此转换器处理的目标类型：{@code String.class}。
     *
     * @return {@code String.class} / {@code String.class}
     */
    @Override
    public Class<String> targetType() {
        return String.class;
    }
}
