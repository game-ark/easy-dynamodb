package com.jojo.framework.easydynamodb.converter.builtin;

import com.jojo.framework.easydynamodb.converter.AttributeConverter;
import com.jojo.framework.easydynamodb.exception.DynamoConversionException;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;

/**
 * Converts LocalDateTime ↔ DynamoDB S (String) type using ISO-8601 format.
 * 使用 ISO-8601 格式转换 LocalDateTime ↔ DynamoDB S（字符串）类型。
 */
public class LocalDateTimeConverter implements AttributeConverter<LocalDateTime> {

    /**
     * Converts a LocalDateTime value to a DynamoDB S (String) AttributeValue in ISO-8601 format.
     * 将 LocalDateTime 值以 ISO-8601 格式转换为 DynamoDB S（字符串）AttributeValue。
     *
     * @param value the LocalDateTime value to convert / 要转换的 LocalDateTime 值
     * @return the DynamoDB S AttributeValue / DynamoDB S 类型的 AttributeValue
     */
    @Override
    public AttributeValue toAttributeValue(LocalDateTime value) {
        return AttributeValue.builder().s(value.toString()).build();
    }

    /**
     * Converts a DynamoDB S (String) AttributeValue back to a LocalDateTime by parsing ISO-8601.
     * 通过解析 ISO-8601 将 DynamoDB S（字符串）AttributeValue 转换回 LocalDateTime。
     *
     * @param attributeValue the DynamoDB AttributeValue to convert / 要转换的 DynamoDB AttributeValue
     * @return the parsed LocalDateTime value / 解析后的 LocalDateTime 值
     */
    @Override
    public LocalDateTime fromAttributeValue(AttributeValue attributeValue) {
        try {
            return LocalDateTime.parse(attributeValue.s());
        } catch (DateTimeParseException e) {
            throw new DynamoConversionException("unknown", String.class, LocalDateTime.class, e);
        }
    }

    /**
     * Returns the target type this converter handles: {@code LocalDateTime.class}.
     * 返回此转换器处理的目标类型：{@code LocalDateTime.class}。
     *
     * @return {@code LocalDateTime.class} / {@code LocalDateTime.class}
     */
    @Override
    public Class<LocalDateTime> targetType() {
        return LocalDateTime.class;
    }
}
