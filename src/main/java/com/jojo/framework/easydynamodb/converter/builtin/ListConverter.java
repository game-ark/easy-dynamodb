package com.jojo.framework.easydynamodb.converter.builtin;

import com.jojo.framework.easydynamodb.converter.AttributeConverter;
import com.jojo.framework.easydynamodb.exception.DynamoConversionException;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Converts List ↔ DynamoDB L (List) type with recursive element conversion.
 * Uses a converter lookup function to resolve element converters at runtime.
 */
public class ListConverter implements AttributeConverter<List<?>> {

    private final Function<Class<?>, AttributeConverter<?>> converterLookup;

    public ListConverter(Function<Class<?>, AttributeConverter<?>> converterLookup) {
        this.converterLookup = converterLookup;
    }

    @Override
    @SuppressWarnings("unchecked")
    public AttributeValue toAttributeValue(List<?> value) {
        List<AttributeValue> items = new ArrayList<>(value.size());
        for (Object element : value) {
            if (element == null) {
                items.add(AttributeValue.builder().nul(true).build());
            } else {
                AttributeConverter<Object> elementConverter =
                        (AttributeConverter<Object>) converterLookup.apply(element.getClass());
                if (elementConverter == null) {
                    throw new DynamoConversionException("list-element",
                            element.getClass(), AttributeValue.class);
                }
                items.add(elementConverter.toAttributeValue(element));
            }
        }
        return AttributeValue.builder().l(items).build();
    }

    @Override
    public List<?> fromAttributeValue(AttributeValue attributeValue) {
        if (!attributeValue.hasL()) {
            throw new DynamoConversionException("list-field",
                    AttributeValue.class, List.class);
        }
        List<AttributeValue> items = attributeValue.l();
        List<Object> result = new ArrayList<>(items.size());
        for (AttributeValue item : items) {
            result.add(AttributeValueExtractor.extractValue(item, converterLookup));
        }
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<List<?>> targetType() {
        return (Class<List<?>>) (Class<?>) List.class;
    }
}
