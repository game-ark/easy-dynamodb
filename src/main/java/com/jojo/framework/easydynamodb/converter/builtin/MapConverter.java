package com.jojo.framework.easydynamodb.converter.builtin;

import com.jojo.framework.easydynamodb.converter.AttributeConverter;
import com.jojo.framework.easydynamodb.exception.DynamoConversionException;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Converts Map&lt;String, ?&gt; ↔ DynamoDB M (Map) type with recursive value conversion.
 * 递归转换 Map&lt;String, ?&gt; ↔ DynamoDB M（映射）类型。
 */
public class MapConverter implements AttributeConverter<Map<String, ?>> {

    private final Function<Class<?>, AttributeConverter<?>> converterLookup;

    /**
     * Creates a MapConverter with the given converter lookup function.
     * 使用给定的转换器查找函数创建 MapConverter。
     *
     * @param converterLookup function to resolve converters for value types / 用于解析值类型转换器的函数
     */
    public MapConverter(Function<Class<?>, AttributeConverter<?>> converterLookup) {
        this.converterLookup = converterLookup;
    }

    /**
     * Converts a Map&lt;String, ?&gt; to a DynamoDB M (Map) AttributeValue, recursively converting each value.
     * Null values are stored as DynamoDB NULL.
     * 将 Map&lt;String, ?&gt; 转换为 DynamoDB M（映射）AttributeValue，递归转换每个值。
     * null 值存储为 DynamoDB NULL。
     *
     * @param value the Map to convert / 要转换的 Map
     * @return the DynamoDB M AttributeValue / DynamoDB M 类型的 AttributeValue
     */
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

    /**
     * Converts a DynamoDB M (Map) AttributeValue back to a Map&lt;String, ?&gt;, recursively extracting each value.
     * 将 DynamoDB M（映射）AttributeValue 转换回 Map&lt;String, ?&gt;，递归提取每个值。
     *
     * @param attributeValue the DynamoDB AttributeValue to convert / 要转换的 DynamoDB AttributeValue
     * @return the extracted Map / 提取的 Map
     */
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

    /**
     * Returns the target type this converter handles: {@code Map.class}.
     * 返回此转换器处理的目标类型：{@code Map.class}。
     *
     * @return {@code Map.class} / {@code Map.class}
     */
    @Override
    @SuppressWarnings("unchecked")
    public Class<Map<String, ?>> targetType() {
        return (Class<Map<String, ?>>) (Class<?>) Map.class;
    }
}
