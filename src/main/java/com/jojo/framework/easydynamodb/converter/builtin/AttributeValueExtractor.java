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
 * 从 DynamoDB AttributeValue 中提取 Java 值的共享工具类。
 * 消除 ListConverter 和 MapConverter 中重复的 extractValue() 逻辑。
 */
public final class AttributeValueExtractor {

    /**
     * Private constructor to prevent instantiation of this utility class.
     * 私有构造函数，防止实例化此工具类。
     */
    private AttributeValueExtractor() {}

    /**
     * Extracts a Java value from an AttributeValue by inspecting its type
     * using {@code av.type()} to avoid AWS SDK v2's non-null default returns.
     * 通过使用 {@code av.type()} 检查类型，从 AttributeValue 中提取 Java 值，
     * 以避免 AWS SDK v2 的非空默认返回值。
     *
     * @param av              the AttributeValue to extract / 要提取的 AttributeValue
     * @param converterLookup function to look up converters for nested types / 用于查找嵌套类型转换器的函数
     * @return the extracted Java value, or null / 提取的 Java 值，或 null
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
