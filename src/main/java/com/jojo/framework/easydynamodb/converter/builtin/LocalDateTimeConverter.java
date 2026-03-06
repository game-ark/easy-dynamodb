package com.jojo.framework.easydynamodb.converter.builtin;

import com.jojo.framework.easydynamodb.converter.AttributeConverter;
import com.jojo.framework.easydynamodb.exception.DynamoConversionException;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;

/**
 * Converts LocalDateTime ↔ DynamoDB S (String) type using ISO-8601 format.
 */
public class LocalDateTimeConverter implements AttributeConverter<LocalDateTime> {

    @Override
    public AttributeValue toAttributeValue(LocalDateTime value) {
        return AttributeValue.builder().s(value.toString()).build();
    }

    @Override
    public LocalDateTime fromAttributeValue(AttributeValue attributeValue) {
        try {
            return LocalDateTime.parse(attributeValue.s());
        } catch (DateTimeParseException e) {
            throw new DynamoConversionException("unknown", String.class, LocalDateTime.class, e);
        }
    }

    @Override
    public Class<LocalDateTime> targetType() {
        return LocalDateTime.class;
    }
}
