package com.jojo.framework.easydynamodb.converter.builtin;

import com.jojo.framework.easydynamodb.converter.AttributeConverter;
import com.jojo.framework.easydynamodb.exception.DynamoConversionException;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.math.BigDecimal;

/**
 * Converts numeric types (Integer, Long, Double, Float, BigDecimal) ↔ DynamoDB N (Number) type.
 * DynamoDB stores all numbers as string representations in the N type.
 * 转换数值类型（Integer、Long、Double、Float、BigDecimal）↔ DynamoDB N（数字）类型。
 * DynamoDB 将所有数字以字符串形式存储在 N 类型中。
 */
public class NumberConverter implements AttributeConverter<Number> {

    private final Class<? extends Number> numberType;

    /**
     * Creates a NumberConverter for the specified numeric type.
     * 为指定的数值类型创建 NumberConverter。
     *
     * @param numberType the concrete Number subclass (e.g. Integer.class, Long.class) / 具体的 Number 子类（如 Integer.class、Long.class）
     */
    public NumberConverter(Class<? extends Number> numberType) {
        this.numberType = numberType;
    }

    /**
     * Converts a Number value to a DynamoDB N (Number) AttributeValue.
     * 将 Number 值转换为 DynamoDB N（数字）AttributeValue。
     *
     * @param value the Number value to convert / 要转换的 Number 值
     * @return the DynamoDB N AttributeValue / DynamoDB N 类型的 AttributeValue
     */
    @Override
    public AttributeValue toAttributeValue(Number value) {
        return AttributeValue.builder().n(value.toString()).build();
    }

    /**
     * Converts a DynamoDB N (Number) AttributeValue back to the target Number type.
     * 将 DynamoDB N（数字）AttributeValue 转换回目标 Number 类型。
     *
     * @param attributeValue the DynamoDB AttributeValue to convert / 要转换的 DynamoDB AttributeValue
     * @return the parsed Number value of the target type / 解析后的目标类型 Number 值
     */
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

    /**
     * Returns the target type this converter handles.
     * 返回此转换器处理的目标类型。
     *
     * @return the Number subclass this converter was created for / 此转换器所创建的 Number 子类
     */
    @Override
    @SuppressWarnings("unchecked")
    public Class<Number> targetType() {
        return (Class<Number>) (Class<?>) numberType;
    }
}
