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
 * <p>
 * Also exposed as a public API so users can use it in expression value maps
 * without the verbose {@code AttributeValue.builder().s("...").build()} pattern.
 */
public final class AttributeValues {

    private AttributeValues() {}

    /**
     * Converts a Java object to a DynamoDB AttributeValue by inspecting its runtime type.
     * <p>
     * Supported types:
     * <ul>
     *   <li>String → S</li>
     *   <li>Number (Integer, Long, Double, Float, Short, Byte, BigDecimal) → N</li>
     *   <li>Boolean → BOOL</li>
     *   <li>byte[] → B</li>
     *   <li>Instant, LocalDateTime → S (ISO-8601)</li>
     *   <li>Enum → S (via name())</li>
     *   <li>AttributeValue → passed through as-is</li>
     *   <li>null → NUL</li>
     * </ul>
     *
     * @param value the Java value to convert
     * @return the corresponding DynamoDB AttributeValue
     * @throws DynamoException if the type is not supported
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
