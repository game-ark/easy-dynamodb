package com.jojo.framework.easydynamodb.converter.builtin;

import com.jojo.framework.easydynamodb.converter.AttributeConverter;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Converts Boolean ↔ DynamoDB BOOL type.
 */
public class BooleanConverter implements AttributeConverter<Boolean> {

    @Override
    public AttributeValue toAttributeValue(Boolean value) {
        return AttributeValue.builder().bool(value).build();
    }

    @Override
    public Boolean fromAttributeValue(AttributeValue attributeValue) {
        return attributeValue.bool();
    }

    @Override
    public Class<Boolean> targetType() {
        return Boolean.class;
    }
}
