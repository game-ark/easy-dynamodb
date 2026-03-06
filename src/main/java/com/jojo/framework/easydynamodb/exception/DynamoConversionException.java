package com.jojo.framework.easydynamodb.exception;

/**
 * Exception thrown when type conversion between Java types and DynamoDB AttributeValue fails.
 * Contains the field name, source type, and target type for diagnostic purposes.
 */
public class DynamoConversionException extends DynamoException {

    private final String fieldName;
    private final Class<?> sourceType;
    private final Class<?> targetType;

    public DynamoConversionException(String fieldName, Class<?> sourceType, Class<?> targetType) {
        super(String.format("Failed to convert field '%s' from %s to %s",
                fieldName, sourceType.getName(), targetType.getName()));
        this.fieldName = fieldName;
        this.sourceType = sourceType;
        this.targetType = targetType;
    }

    public DynamoConversionException(String fieldName, Class<?> sourceType, Class<?> targetType, Throwable cause) {
        super(String.format("Failed to convert field '%s' from %s to %s",
                fieldName, sourceType.getName(), targetType.getName()), cause);
        this.fieldName = fieldName;
        this.sourceType = sourceType;
        this.targetType = targetType;
    }

    public String getFieldName() {
        return fieldName;
    }

    public Class<?> getSourceType() {
        return sourceType;
    }

    public Class<?> getTargetType() {
        return targetType;
    }
}
