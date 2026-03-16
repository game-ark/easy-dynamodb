package com.jojo.framework.easydynamodb.converter;

import com.jojo.framework.easydynamodb.converter.builtin.*;
import com.jojo.framework.easydynamodb.logging.DdmLogger;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Registry for type-to-converter mappings. Initializes with all built-in converters
 * and supports custom converter registration. Caches converters per Java type to
 * avoid runtime lookup overhead.
 * 类型到转换器映射的注册中心。初始化时注册所有内置转换器，并支持自定义转换器注册。
 * 按 Java 类型缓存转换器，以避免运行时查找开销。
 */
public class ConverterRegistry {

    private static final DdmLogger log = DdmLogger.getLogger(ConverterRegistry.class);

    private final Map<Class<?>, AttributeConverter<?>> converters = new ConcurrentHashMap<>();

    /**
     * Creates a new ConverterRegistry with all built-in converters registered.
     * 创建一个新的 ConverterRegistry，并注册所有内置转换器。
     */
    public ConverterRegistry() {
        registerBuiltinConverters();
    }

    /**
     * Registers a custom converter for the given type, overriding any existing mapping.
     * 为指定类型注册自定义转换器，覆盖任何已有的映射。
     *
     * @param type      the Java class to associate with the converter / 要与转换器关联的 Java 类
     * @param converter the converter instance / 转换器实例
     */
    public void register(Class<?> type, AttributeConverter<?> converter) {
        converters.put(type, converter);
        log.debug("Registered converter for type {}: {}", type.getSimpleName(), converter.getClass().getSimpleName());
    }

    /**
     * Returns the converter for the given type, or null if none is registered.
     * 返回指定类型的转换器，如果未注册则返回 null。
     *
     * @param type the Java class to look up / 要查找的 Java 类
     * @return the associated converter, or null / 关联的转换器，如果没有则为 null
     */
    public AttributeConverter<?> getConverter(Class<?> type) {
        return converters.get(type);
    }

    /**
     * Returns a lookup function suitable for passing to ListConverter and MapConverter.
     * 返回一个查找函数，适合传递给 ListConverter 和 MapConverter。
     *
     * @return a function that maps a Java class to its converter / 将 Java 类映射到其转换器的函数
     */
    public Function<Class<?>, AttributeConverter<?>> lookupFunction() {
        return this::getConverter;
    }

    private void registerBuiltinConverters() {
        // String
        converters.put(String.class, new StringConverter());

        // Numeric types — boxed and primitive
        NumberConverter intConverter = new NumberConverter(Integer.class);
        converters.put(Integer.class, intConverter);
        converters.put(int.class, intConverter);

        NumberConverter longConverter = new NumberConverter(Long.class);
        converters.put(Long.class, longConverter);
        converters.put(long.class, longConverter);

        NumberConverter doubleConverter = new NumberConverter(Double.class);
        converters.put(Double.class, doubleConverter);
        converters.put(double.class, doubleConverter);

        NumberConverter floatConverter = new NumberConverter(Float.class);
        converters.put(Float.class, floatConverter);
        converters.put(float.class, floatConverter);

        NumberConverter shortConverter = new NumberConverter(Short.class);
        converters.put(Short.class, shortConverter);
        converters.put(short.class, shortConverter);

        NumberConverter byteConverter = new NumberConverter(Byte.class);
        converters.put(Byte.class, byteConverter);
        converters.put(byte.class, byteConverter);

        converters.put(BigDecimal.class, new NumberConverter(BigDecimal.class));

        // Boolean — boxed and primitive
        BooleanConverter boolConverter = new BooleanConverter();
        converters.put(Boolean.class, boolConverter);
        converters.put(boolean.class, boolConverter);

        // Binary
        converters.put(byte[].class, new BinaryConverter());

        // Temporal types
        converters.put(Instant.class, new InstantConverter());
        converters.put(LocalDateTime.class, new LocalDateTimeConverter());

        // Collection types (use this::getConverter as the lookup function)
        converters.put(List.class, new ListConverter(this::getConverter));
        converters.put(Map.class, new MapConverter(this::getConverter));

        // Set defaults to STRING_SET; per-field resolution happens in MetadataRegistry
        converters.put(Set.class, new SetConverter(SetConverter.SetType.STRING_SET));
    }
}
