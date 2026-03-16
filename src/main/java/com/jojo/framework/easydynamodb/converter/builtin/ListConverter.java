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
 * 递归转换 List ↔ DynamoDB L（列表）类型。
 * 使用转换器查找函数在运行时解析元素转换器。
 */
public class ListConverter implements AttributeConverter<List<?>> {

    private final Function<Class<?>, AttributeConverter<?>> converterLookup;

    /**
     * Creates a ListConverter with the given converter lookup function.
     * 使用给定的转换器查找函数创建 ListConverter。
     *
     * @param converterLookup function to resolve converters for element types / 用于解析元素类型转换器的函数
     */
    public ListConverter(Function<Class<?>, AttributeConverter<?>> converterLookup) {
        this.converterLookup = converterLookup;
    }

    /**
     * Converts a List to a DynamoDB L (List) AttributeValue, recursively converting each element.
     * Null elements are stored as DynamoDB NULL.
     * 将 List 转换为 DynamoDB L（列表）AttributeValue，递归转换每个元素。
     * null 元素存储为 DynamoDB NULL。
     *
     * @param value the List to convert / 要转换的 List
     * @return the DynamoDB L AttributeValue / DynamoDB L 类型的 AttributeValue
     */
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

    /**
     * Converts a DynamoDB L (List) AttributeValue back to a List, recursively extracting each element.
     * 将 DynamoDB L（列表）AttributeValue 转换回 List，递归提取每个元素。
     *
     * @param attributeValue the DynamoDB AttributeValue to convert / 要转换的 DynamoDB AttributeValue
     * @return the extracted List / 提取的 List
     */
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

    /**
     * Returns the target type this converter handles: {@code List.class}.
     * 返回此转换器处理的目标类型：{@code List.class}。
     *
     * @return {@code List.class} / {@code List.class}
     */
    @Override
    @SuppressWarnings("unchecked")
    public Class<List<?>> targetType() {
        return (Class<List<?>>) (Class<?>) List.class;
    }
}
