package com.jojo.framework.easydynamodb.converter.builtin;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.math.BigDecimal;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Function;

import com.jojo.framework.easydynamodb.converter.AttributeConverter;

/**
 * Shared utility for extracting Java values from DynamoDB AttributeValue.
 * Eliminates duplicate extractValue() logic in ListConverter and MapConverter.
 */
public final class AttributeValueExtractor {

    private AttributeValueExtractor() {}

    /**
     * Extracts a Java value from an AttributeValue by inspecting its type
     * using {@code av.type()} to avoid AWS SDK v2's non-null default returns.
     *
     * @param av              the AttributeValue to extract
     * @param converterLookup function to look up converters for nested types
     * @return the extracted Java value, or null
     */
    public static Object extractValue(AttributeValue av,
                                       Function<Class<?>, AttributeConverter<?>> converterLookup) {
        if (av == null || Boolean.TRUE.equals(av.nul())) {
            return null;
        }
        return switch (av.type()) {
            case S -> av.s();
            case N -> new BigDecimal(av.n());
            case BOOL -> av.bool();
            case B -> av.b().asByteArray();
            case L -> new ListConverter(converterLookup).fromAttributeValue(av);
            case M -> new MapConverter(converterLookup).fromAttributeValue(av);
            case SS -> new LinkedHashSet<>(av.ss());
            case NS -> {
                Set<BigDecimal> nums = new LinkedHashSet<>();
                for (String n : av.ns()) {
                    nums.add(new BigDecimal(n));
                }
                yield nums;
            }
            case NUL -> null;
            default -> null;
        };
    }
}
