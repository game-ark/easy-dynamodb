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
        List<AttributeValue> items = attributeValue.l();
        List<Object> result = new ArrayList<>(items.size());
        for (AttributeValue item : items) {
            result.add(extractValue(item));
        }
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<List<?>> targetType() {
        return (Class<List<?>>) (Class<?>) List.class;
    }

    /**
     * Extracts a Java value from an AttributeValue by inspecting its type.
     */
    private Object extractValue(AttributeValue av) {
        if (Boolean.TRUE.equals(av.nul())) {
            return null;
        }
        if (av.s() != null) {
            return av.s();
        }
        if (av.n() != null) {
            return new java.math.BigDecimal(av.n());
        }
        if (av.bool() != null) {
            return av.bool();
        }
        if (av.b() != null) {
            return av.b().asByteArray();
        }
        if (av.hasL()) {
            return fromAttributeValue(av);
        }
        if (av.hasM()) {
            MapConverter mapConverter = new MapConverter(converterLookup);
            return mapConverter.fromAttributeValue(av);
        }
        if (av.hasSs()) {
            return new java.util.LinkedHashSet<>(av.ss());
        }
        if (av.hasNs()) {
            java.util.Set<java.math.BigDecimal> nums = new java.util.LinkedHashSet<>();
            for (String n : av.ns()) {
                nums.add(new java.math.BigDecimal(n));
            }
            return nums;
        }
        return null;
    }
}
