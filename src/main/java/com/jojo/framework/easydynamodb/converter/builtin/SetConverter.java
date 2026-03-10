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
 */
public class SetConverter implements AttributeConverter<Set<?>> {

    public enum SetType {
        STRING_SET,
        NUMBER_SET
    }

    private final SetType setType;
    private final Class<?> elementType;

    public SetConverter(SetType setType) {
        this(setType, null);
    }

    /**
     * Creates a SetConverter with explicit element type for correct deserialization.
     * For NUMBER_SET, the elementType determines whether values are parsed as
     * Integer, Long, Double, etc. instead of always returning BigDecimal.
     *
     * @param setType     STRING_SET or NUMBER_SET
     * @param elementType the Java type of set elements (e.g. Integer.class), nullable
     */
    public SetConverter(SetType setType, Class<?> elementType) {
        this.setType = setType;
        this.elementType = elementType;
    }

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

    @Override
    @SuppressWarnings("unchecked")
    public Class<Set<?>> targetType() {
        return (Class<Set<?>>) (Class<?>) Set.class;
    }
}
