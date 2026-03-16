package com.jojo.framework.easydynamodb.converter.builtin;

import com.jojo.framework.easydynamodb.converter.AttributeConverter;
import com.jojo.framework.easydynamodb.exception.DynamoConversionException;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Converts Enum types ↔ DynamoDB S (String) type using {@code name()}.
 * Automatically used for any Enum field without explicit converter registration.
 * 使用 {@code name()} 转换枚举类型 ↔ DynamoDB S（字符串）类型。
 * 对于没有显式注册转换器的枚举字段，将自动使用此转换器。
 *
 * @param <E> the enum type / 枚举类型
 */
public class EnumConverter<E extends Enum<E>> implements AttributeConverter<E> {

    private final Class<E> enumClass;

    /**
     * Creates an EnumConverter for the specified enum class.
     * 为指定的枚举类创建 EnumConverter。
     *
     * @param enumClass the enum class to convert / 要转换的枚举类
     */
    public EnumConverter(Class<E> enumClass) {
        this.enumClass = enumClass;
    }

    /**
     * Converts an enum value to a DynamoDB S (String) AttributeValue using {@code name()}.
     * 使用 {@code name()} 将枚举值转换为 DynamoDB S（字符串）AttributeValue。
     *
     * @param value the enum value to convert / 要转换的枚举值
     * @return the DynamoDB S AttributeValue containing the enum name / 包含枚举名称的 DynamoDB S AttributeValue
     */
    @Override
    public AttributeValue toAttributeValue(E value) {
        return AttributeValue.builder().s(value.name()).build();
    }

    /**
     * Converts a DynamoDB S (String) AttributeValue back to the enum constant.
     * 将 DynamoDB S（字符串）AttributeValue 转换回枚举常量。
     *
     * @param attributeValue the DynamoDB AttributeValue to convert / 要转换的 DynamoDB AttributeValue
     * @return the corresponding enum constant / 对应的枚举常量
     */
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

    /**
     * Returns the target enum class this converter handles.
     * 返回此转换器处理的目标枚举类。
     *
     * @return the enum class / 枚举类
     */
    @Override
    public Class<E> targetType() {
        return enumClass;
    }
}
