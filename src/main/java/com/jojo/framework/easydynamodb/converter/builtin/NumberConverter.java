package com.jojo.framework.easydynamodb.converter.builtin;

import com.jojo.framework.easydynamodb.converter.AttributeConverter;
import com.jojo.framework.easydynamodb.exception.DynamoConversionException;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.math.BigDecimal;

/**
 * Converts numeric types (Integer, Long, Double, Float, BigDecimal) ↔ DynamoDB N (Number) type.
 * DynamoDB stores all numbers as string representations in the N type.
 */
public class NumberConverter implements AttributeConverter<Number> {

    private final Class<? extends Number> numberType;

    public NumberConverter(Class<? extends Number> numberType) {
        this.numberType = numberType;
    }

    @Override
    public AttributeValue toAttributeValue(Number value) {
        return AttributeValue.builder().n(value.toString()).build();
    }

    @Override
    public Number fromAttributeValue(AttributeValue attributeValue) {
        String n = attributeValue.n();
        try {
            if (numberType == Integer.class) {
                return Integer.parseInt(n);
            } else if (numberType == Long.class) {
                return Long.parseLong(n);
            } else if (numberType == Double.class) {
                return Double.parseDouble(n);
            } else if (numberType == Float.class) {
                return Float.parseFloat(n);
            } else if (numberType == BigDecimal.class) {
                return new BigDecimal(n);
            }
            throw new DynamoConversionException("unknown", numberType, Number.class);
        } catch (NumberFormatException e) {
            throw new DynamoConversionException("unknown", String.class, numberType, e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<Number> targetType() {
        return (Class<Number>) (Class<?>) numberType;
    }
}
