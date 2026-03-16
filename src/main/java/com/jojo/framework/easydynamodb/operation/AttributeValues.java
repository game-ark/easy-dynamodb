package com.jojo.framework.easydynamodb.operation;

import com.jojo.framework.easydynamodb.exception.DynamoException;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;

/**
 * Utility for converting common Java types to DynamoDB AttributeValue.
 * Used internally by QueryBuilder/ScanBuilder's {@code value()} shorthand methods.
 * 用于将常见 Java 类型转换为 DynamoDB AttributeValue 的工具类。
 * 内部由 QueryBuilder/ScanBuilder 的 {@code value()} 简写方法使用。
 * <p>
 * Also exposed as a public API so users can use it in expression value maps
 * without the verbose {@code AttributeValue.builder().s("...").build()} pattern.
 * 同时作为公共 API 暴露，用户可在表达式值映射中使用，
 * 无需冗长的 {@code AttributeValue.builder().s("...").build()} 模式。
 */
public final class AttributeValues {

    private AttributeValues() {}

    /**
     * Converts a Java object to a DynamoDB AttributeValue by inspecting its runtime type.
     * 通过检查运行时类型将 Java 对象转换为 DynamoDB AttributeValue。
     * <p>
     * Supported types:
     * 支持的类型：
     * <ul>
     *   <li>String → S</li>
     *   <li>Number (Integer, Long, Double, Float, Short, Byte, BigDecimal) → N / 数字类型 → N</li>
     *   <li>Boolean → BOOL / 布尔值 → BOOL</li>
     *   <li>byte[] → B</li>
     *   <li>Instant, LocalDateTime → S (ISO-8601)</li>
     *   <li>Enum → S (via name()) / 枚举 → S（通过 name()）</li>
     *   <li>AttributeValue → passed through as-is / 直接透传</li>
     *   <li>null → NUL</li>
     * </ul>
     *
     * @param value the Java value to convert / 要转换的 Java 值
     * @return the corresponding DynamoDB AttributeValue / 对应的 DynamoDB AttributeValue
     * @throws DynamoException if the type is not supported / 类型不支持时抛出
     */
    public static AttributeValue of(Object value) {
        if (value == null) {
            return AttributeValue.builder().nul(true).build();
        }
        if (value instanceof AttributeValue av) {
            return av;
        }
        if (value instanceof String s) {
            return AttributeValue.builder().s(s).build();
        }
        if (value instanceof Number n) {
            return AttributeValue.builder().n(n.toString()).build();
        }
        if (value instanceof Boolean b) {
            return AttributeValue.builder().bool(b).build();
        }
        if (value instanceof byte[] bytes) {
            return AttributeValue.builder().b(SdkBytes.fromByteArray(bytes)).build();
        }
        if (value instanceof Instant instant) {
            return AttributeValue.builder().s(instant.toString()).build();
        }
        if (value instanceof LocalDateTime ldt) {
            return AttributeValue.builder().s(ldt.toString()).build();
        }
        if (value instanceof Enum<?> e) {
            return AttributeValue.builder().s(e.name()).build();
        }
        throw new DynamoException(
                "Cannot auto-convert type " + value.getClass().getName() + " to AttributeValue. "
                        + "Use AttributeValue.builder() directly or register a custom converter.");
    }
}
