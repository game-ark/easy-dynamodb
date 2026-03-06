package com.jojo.framework.easydynamodb.converter.builtin;

import com.jojo.framework.easydynamodb.converter.AttributeConverter;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Converts String ↔ DynamoDB S (String) type.
 */
public class StringConverter implements AttributeConverter<String> {

    @Override
    public AttributeValue toAttributeValue(String value) {
        return AttributeValue.builder().s(value).build();
    }

    @Override
    public String fromAttributeValue(AttributeValue attributeValue) {
        return attributeValue.s();
    }

    @Override
    public Class<String> targetType() {
        return String.class;
    }
}
