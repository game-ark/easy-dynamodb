package com.jojo.framework.easydynamodb.converter.builtin;

import com.jojo.framework.easydynamodb.converter.AttributeConverter;
import com.jojo.framework.easydynamodb.exception.DynamoConversionException;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.math.BigDecimal;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Converts Set types ↔ DynamoDB SS (String Set) or NS (Number Set) types.
 * <p>
 * Set&lt;String&gt; maps to SS, Set&lt;Number&gt; (Integer, Long, Double, Float, BigDecimal) maps to NS.
 * 转换 Set 类型 ↔ DynamoDB SS（字符串集合）或 NS（数字集合）类型。
 * <p>
 * Set&lt;String&gt; 映射到 SS，Set&lt;Number&gt;（Integer、Long、Double、Float、BigDecimal）映射到 NS。
 */
public class SetConverter implements AttributeConverter<Set<?>> {

    /**
     * Enum representing the DynamoDB set type.
     * 表示 DynamoDB 集合类型的枚举。
     */
    public enum SetType {
        STRING_SET,
        NUMBER_SET
    }

    private final SetType setType;
    private final Class<?> elementType;

    /**
     * Creates a SetConverter for the specified set type with no explicit element type.
     * 为指定的集合类型创建 SetConverter，不指定显式元素类型。
     *
     * @param setType STRING_SET or NUMBER_SET / STRING_SET 或 NUMBER_SET
     */
    public SetConverter(SetType setType) {
        this(setType, null);
    }

    /**
     * Creates a SetConverter with explicit element type for correct deserialization.
     * For NUMBER_SET, the elementType determines whether values are parsed as
     * Integer, Long, Double, etc. instead of always returning BigDecimal.
     * 使用显式元素类型创建 SetConverter，以确保正确的反序列化。
     * 对于 NUMBER_SET，elementType 决定值是否解析为 Integer、Long、Double 等，
     * 而不是始终返回 BigDecimal。
     *
     * @param setType     STRING_SET or NUMBER_SET / STRING_SET 或 NUMBER_SET
     * @param elementType the Java type of set elements (e.g. Integer.class), nullable / 集合元素的 Java 类型（如 Integer.class），可为 null
     */
    public SetConverter(SetType setType, Class<?> elementType) {
        this.setType = setType;
        this.elementType = elementType;
    }

    /**
     * Converts a Set to a DynamoDB SS (String Set) or NS (Number Set) AttributeValue.
     * Empty sets are stored as DynamoDB NULL since DynamoDB does not allow empty SS/NS.
     * 将 Set 转换为 DynamoDB SS（字符串集合）或 NS（数字集合）AttributeValue。
     * 空集合存储为 DynamoDB NULL，因为 DynamoDB 不允许空的 SS/NS。
     *
     * @param value the Set to convert / 要转换的 Set
     * @return the DynamoDB SS or NS AttributeValue, or NULL for empty sets / DynamoDB SS 或 NS AttributeValue，空集合返回 NULL
     */
    @Override
    public AttributeValue toAttributeValue(Set<?> value) {
        if (value.isEmpty()) {
            // Store empty sets as NULL with a convention marker.
            // DynamoDB does not allow empty SS/NS, and using empty L is ambiguous
            // with actual empty lists. NULL is the safest representation.
            return AttributeValue.builder().nul(true).build();
        }
        return switch (setType) {
            case STRING_SET -> {
                Set<String> strings = value.stream()
                        .map(Object::toString)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
                yield AttributeValue.builder().ss(strings).build();
            }
            case NUMBER_SET -> {
                Set<String> numbers = value.stream()
                        .map(Object::toString)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
                yield AttributeValue.builder().ns(numbers).build();
            }
        };
    }

    /**
     * Converts a DynamoDB SS (String Set) or NS (Number Set) AttributeValue back to a Set.
     * DynamoDB NULL is interpreted as an empty set.
     * 将 DynamoDB SS（字符串集合）或 NS（数字集合）AttributeValue 转换回 Set。
     * DynamoDB NULL 被解释为空集合。
     *
     * @param attributeValue the DynamoDB AttributeValue to convert / 要转换的 DynamoDB AttributeValue
     * @return the extracted Set / 提取的 Set
     */
    @Override
    public Set<?> fromAttributeValue(AttributeValue attributeValue) {
        // Empty set was stored as NULL
        if (Boolean.TRUE.equals(attributeValue.nul())) {
            return new LinkedHashSet<>();
        }
        return switch (setType) {
            case STRING_SET -> {
                if (attributeValue.hasSs()) {
                    yield new LinkedHashSet<>(attributeValue.ss());
                }
                throw new DynamoConversionException("set-field",
                        AttributeValue.class, Set.class);
            }
            case NUMBER_SET -> {
                if (attributeValue.hasNs()) {
                    yield attributeValue.ns().stream()
                            .map(n -> parseNumber(n, elementType))
                            .collect(Collectors.toCollection(LinkedHashSet::new));
                }
                throw new DynamoConversionException("set-field",
                        AttributeValue.class, Set.class);
            }
        };
    }

    /**
     * Parses a number string to the correct Java type based on elementType.
     * Falls back to BigDecimal if elementType is null or unrecognized.
     * 根据 elementType 将数字字符串解析为正确的 Java 类型。
     * 如果 elementType 为 null 或无法识别，则回退为 BigDecimal。
     *
     * @param n    the number string to parse / 要解析的数字字符串
     * @param type the target Java type for the number / 数字的目标 Java 类型
     * @return the parsed Number value / 解析后的 Number 值
     */
    private static Number parseNumber(String n, Class<?> type) {
        if (type == Integer.class || type == int.class) return Integer.parseInt(n);
        if (type == Long.class || type == long.class) return Long.parseLong(n);
        if (type == Double.class || type == double.class) return Double.parseDouble(n);
        if (type == Float.class || type == float.class) return Float.parseFloat(n);
        if (type == Short.class || type == short.class) return Short.parseShort(n);
        if (type == Byte.class || type == byte.class) return Byte.parseByte(n);
        return new BigDecimal(n);
    }

    /**
     * Returns the target type this converter handles: {@code Set.class}.
     * 返回此转换器处理的目标类型：{@code Set.class}。
     *
     * @return {@code Set.class} / {@code Set.class}
     */
    @Override
    @SuppressWarnings("unchecked")
    public Class<Set<?>> targetType() {
        return (Class<Set<?>>) (Class<?>) Set.class;
    }
}
