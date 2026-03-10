package com.jojo.framework.easydynamodb.converter.builtin;

import com.jojo.framework.easydynamodb.converter.AttributeConverter;
import com.jojo.framework.easydynamodb.exception.DynamoConversionException;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Converts Enum types ↔ DynamoDB S (String) type using {@code name()}.
 * Automatically used for any Enum field without explicit converter registration.
 *
 * @param <E> the enum type
 */
public class EnumConverter<E extends Enum<E>> implements AttributeConverter<E> {

    private final Class<E> enumClass;

    public EnumConverter(Class<E> enumClass) {
        this.enumClass = enumClass;
    }

    @Override
    public AttributeValue toAttributeValue(E value) {
        return AttributeValue.builder().s(value.name()).build();
    }

    @Override
    public E fromAttributeValue(AttributeValue attributeValue) {
        String name = attributeValue.s();
        try {
            return Enum.valueOf(enumClass, name);
        } catch (IllegalArgumentException e) {
            throw new DynamoConversionException(
                    "enum-field", String.class, enumClass, e);
        }
    }

    @Override
    public Class<E> targetType() {
        return enumClass;
    }
}
