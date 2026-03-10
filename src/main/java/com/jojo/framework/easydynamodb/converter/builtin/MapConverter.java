package com.jojo.framework.easydynamodb.converter.builtin;

import com.jojo.framework.easydynamodb.converter.AttributeConverter;
import com.jojo.framework.easydynamodb.exception.DynamoConversionException;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Converts Map&lt;String, ?&gt; ↔ DynamoDB M (Map) type with recursive value conversion.
 */
public class MapConverter implements AttributeConverter<Map<String, ?>> {

    private final Function<Class<?>, AttributeConverter<?>> converterLookup;

    public MapConverter(Function<Class<?>, AttributeConverter<?>> converterLookup) {
        this.converterLookup = converterLookup;
    }

    @Override
    @SuppressWarnings("unchecked")
    public AttributeValue toAttributeValue(Map<String, ?> value) {
        Map<String, AttributeValue> map = new LinkedHashMap<>();
        for (Map.Entry<String, ?> entry : value.entrySet()) {
            Object v = entry.getValue();
            if (v == null) {
                map.put(entry.getKey(), AttributeValue.builder().nul(true).build());
            } else {
                AttributeConverter<Object> converter =
                        (AttributeConverter<Object>) converterLookup.apply(v.getClass());
                if (converter == null) {
                    throw new DynamoConversionException("map-value[" + entry.getKey() + "]",
                            v.getClass(), AttributeValue.class);
                }
                map.put(entry.getKey(), converter.toAttributeValue(v));
            }
        }
        return AttributeValue.builder().m(map).build();
    }

    @Override
    public Map<String, ?> fromAttributeValue(AttributeValue attributeValue) {
        if (!attributeValue.hasM()) {
            throw new DynamoConversionException("map-field",
                    AttributeValue.class, Map.class);
        }
        Map<String, AttributeValue> m = attributeValue.m();
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<String, AttributeValue> entry : m.entrySet()) {
            result.put(entry.getKey(), AttributeValueExtractor.extractValue(entry.getValue(), converterLookup));
        }
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<Map<String, ?>> targetType() {
        return (Class<Map<String, ?>>) (Class<?>) Map.class;
    }
}
