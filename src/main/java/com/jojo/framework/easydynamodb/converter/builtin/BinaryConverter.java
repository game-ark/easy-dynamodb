package com.jojo.framework.easydynamodb.converter.builtin;

import com.jojo.framework.easydynamodb.converter.AttributeConverter;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Converts byte[] ↔ DynamoDB B (Binary) type.
 * 转换 byte[] ↔ DynamoDB B（二进制）类型。
 */
public class BinaryConverter implements AttributeConverter<byte[]> {

    /**
     * Converts a byte array to a DynamoDB B (Binary) AttributeValue.
     * 将字节数组转换为 DynamoDB B（二进制）AttributeValue。
     *
     * @param value the byte array to convert / 要转换的字节数组
     * @return the DynamoDB B AttributeValue / DynamoDB B 类型的 AttributeValue
     */
    @Override
    public AttributeValue toAttributeValue(byte[] value) {
        return AttributeValue.builder().b(SdkBytes.fromByteArray(value)).build();
    }

    /**
     * Converts a DynamoDB B (Binary) AttributeValue back to a byte array.
     * 将 DynamoDB B（二进制）AttributeValue 转换回字节数组。
     *
     * @param attributeValue the DynamoDB AttributeValue to convert / 要转换的 DynamoDB AttributeValue
     * @return the extracted byte array / 提取的字节数组
     */
    @Override
    public byte[] fromAttributeValue(AttributeValue attributeValue) {
        return attributeValue.b().asByteArray();
    }

    /**
     * Returns the target type this converter handles: {@code byte[].class}.
     * 返回此转换器处理的目标类型：{@code byte[].class}。
     *
     * @return {@code byte[].class} / {@code byte[].class}
     */
    @Override
    public Class<byte[]> targetType() {
        return byte[].class;
    }
}
