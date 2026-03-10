package com.jojo.framework.easydynamodb.metadata;

import com.jojo.framework.easydynamodb.converter.AttributeConverter;
import com.jojo.framework.easydynamodb.exception.DynamoConversionException;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Type;

/**
 * Holds cached metadata for a single entity field, including MethodHandle-based
 * accessors and the bound AttributeConverter. Designed for maximum runtime
 * performance — all reflection work happens once at registration time.
 */
public class FieldMetadata {

    private final String javaFieldName;
    private final String dynamoAttributeName;
    private final Class<?> fieldType;
    private final Type genericType;
    private final MethodHandle getter;
    private final MethodHandle setter;
    private final AttributeConverter<?> converter;
    private final boolean isPartitionKey;
    private final boolean isSortKey;
    private final boolean isIgnored;

    public FieldMetadata(String javaFieldName,
                         String dynamoAttributeName,
                         Class<?> fieldType,
                         Type genericType,
                         MethodHandle getter,
                         MethodHandle setter,
                         AttributeConverter<?> converter,
                         boolean isPartitionKey,
                         boolean isSortKey,
                         boolean isIgnored) {
        this.javaFieldName = javaFieldName;
        this.dynamoAttributeName = dynamoAttributeName;
        this.fieldType = fieldType;
        this.genericType = genericType;
        this.getter = getter;
        this.setter = setter;
        this.converter = converter;
        this.isPartitionKey = isPartitionKey;
        this.isSortKey = isSortKey;
        this.isIgnored = isIgnored;
    }

    /**
     * Reads the field value from the given entity via MethodHandle.
     */
    public Object getValue(Object entity) {
        try {
            return getter.invoke(entity);
        } catch (Throwable e) {
            throw new DynamoConversionException(
                    javaFieldName, entity.getClass(), fieldType, e);
        }
    }

    /**
     * Writes the field value on the given entity via MethodHandle.
     */
    public void setValue(Object entity, Object value) {
        try {
            setter.invoke(entity, value);
        } catch (Throwable e) {
            throw new DynamoConversionException(
                    javaFieldName, value == null ? Void.class : value.getClass(), fieldType, e);
        }
    }

    /**
     * Converts a Java value to a DynamoDB AttributeValue using the bound converter.
     */
    @SuppressWarnings("unchecked")
    public AttributeValue toAttributeValue(Object value) {
        try {
            return ((AttributeConverter<Object>) converter).toAttributeValue(value);
        } catch (DynamoConversionException e) {
            throw e;
        } catch (Exception e) {
            throw new DynamoConversionException(
                    javaFieldName, value == null ? Void.class : value.getClass(),
                    AttributeValue.class, e);
        }
    }

    /**
     * Converts a DynamoDB AttributeValue back to a Java value using the bound converter.
     */
    public Object fromAttributeValue(AttributeValue av) {
        try {
            return converter.fromAttributeValue(av);
        } catch (DynamoConversionException e) {
            // Re-wrap with the correct field name if the converter used a placeholder
            if ("unknown".equals(e.getFieldName()) || "enum-field".equals(e.getFieldName())
                    || "set-field".equals(e.getFieldName())) {
                throw new DynamoConversionException(javaFieldName, e.getSourceType(), e.getTargetType(), e.getCause());
            }
            throw e;
        } catch (Exception e) {
            throw new DynamoConversionException(
                    javaFieldName, AttributeValue.class, fieldType, e);
        }
    }

    // ---- Getters ----

    public String getJavaFieldName() {
        return javaFieldName;
    }

    public String getDynamoAttributeName() {
        return dynamoAttributeName;
    }

    public Class<?> getFieldType() {
        return fieldType;
    }

    public Type getGenericType() {
        return genericType;
    }

    public MethodHandle getGetter() {
        return getter;
    }

    public MethodHandle getSetter() {
        return setter;
    }

    public AttributeConverter<?> getConverter() {
        return converter;
    }

    public boolean isPartitionKey() {
        return isPartitionKey;
    }

    public boolean isSortKey() {
        return isSortKey;
    }

    public boolean isIgnored() {
        return isIgnored;
    }
}
