package com.jojo.framework.easydynamodb.converter.builtin;

import com.jojo.framework.easydynamodb.converter.AttributeConverter;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Converts byte[] ↔ DynamoDB B (Binary) type.
 */
public class BinaryConverter implements AttributeConverter<byte[]> {

    @Override
    public AttributeValue toAttributeValue(byte[] value) {
        return AttributeValue.builder().b(SdkBytes.fromByteArray(value)).build();
    }

    @Override
    public byte[] fromAttributeValue(AttributeValue attributeValue) {
        return attributeValue.b().asByteArray();
    }

    @Override
    public Class<byte[]> targetType() {
        return byte[].class;
    }
}
