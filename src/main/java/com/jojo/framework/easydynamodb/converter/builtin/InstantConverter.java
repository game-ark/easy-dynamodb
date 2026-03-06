package com.jojo.framework.easydynamodb.converter.builtin;

import com.jojo.framework.easydynamodb.converter.AttributeConverter;
import com.jojo.framework.easydynamodb.exception.DynamoConversionException;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Instant;
import java.time.format.DateTimeParseException;

/**
 * Converts Instant ↔ DynamoDB S (String) type using ISO-8601 format.
 */
public class InstantConverter implements AttributeConverter<Instant> {

    @Override
    public AttributeValue toAttributeValue(Instant value) {
        return AttributeValue.builder().s(value.toString()).build();
    }

    @Override
    public Instant fromAttributeValue(AttributeValue attributeValue) {
        try {
            return Instant.parse(attributeValue.s());
        } catch (DateTimeParseException e) {
            throw new DynamoConversionException("unknown", String.class, Instant.class, e);
        }
    }

    @Override
    public Class<Instant> targetType() {
        return Instant.class;
    }
}
