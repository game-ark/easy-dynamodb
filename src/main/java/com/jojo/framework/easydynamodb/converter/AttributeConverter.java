package com.jojo.framework.easydynamodb.converter;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Interface for converting between Java types and DynamoDB AttributeValue.
 *
 * @param <T> the Java type this converter handles
 */
public interface AttributeConverter<T> {
    AttributeValue toAttributeValue(T value);
    T fromAttributeValue(AttributeValue attributeValue);
    Class<T> targetType();
}
