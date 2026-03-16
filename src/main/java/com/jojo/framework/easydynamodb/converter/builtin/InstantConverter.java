package com.jojo.framework.easydynamodb.converter.builtin;

import com.jojo.framework.easydynamodb.converter.AttributeConverter;
import com.jojo.framework.easydynamodb.exception.DynamoConversionException;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Instant;
import java.time.format.DateTimeParseException;

/**
 * Converts Instant ↔ DynamoDB S (String) type using ISO-8601 format.
 * 使用 ISO-8601 格式转换 Instant ↔ DynamoDB S（字符串）类型。
 */
public class InstantConverter implements AttributeConverter<Instant> {

    /**
     * Converts an Instant value to a DynamoDB S (String) AttributeValue in ISO-8601 format.
     * 将 Instant 值以 ISO-8601 格式转换为 DynamoDB S（字符串）AttributeValue。
     *
     * @param value the Instant value to convert / 要转换的 Instant 值
     * @return the DynamoDB S AttributeValue / DynamoDB S 类型的 AttributeValue
     */
    @Override
    public AttributeValue toAttributeValue(Instant value) {
        return AttributeValue.builder().s(value.toString()).build();
    }

    /**
     * Converts a DynamoDB S (String) AttributeValue back to an Instant by parsing ISO-8601.
     * 通过解析 ISO-8601 将 DynamoDB S（字符串）AttributeValue 转换回 Instant。
     *
     * @param attributeValue the DynamoDB AttributeValue to convert / 要转换的 DynamoDB AttributeValue
     * @return the parsed Instant value / 解析后的 Instant 值
     */
    @Override
    public Instant fromAttributeValue(AttributeValue attributeValue) {
        try {
            return Instant.parse(attributeValue.s());
        } catch (DateTimeParseException e) {
            throw new DynamoConversionException("unknown", String.class, Instant.class, e);
        }
    }

    /**
     * Returns the target type this converter handles: {@code Instant.class}.
     * 返回此转换器处理的目标类型：{@code Instant.class}。
     *
     * @return {@code Instant.class} / {@code Instant.class}
     */
    @Override
    public Class<Instant> targetType() {
        return Instant.class;
    }
}
