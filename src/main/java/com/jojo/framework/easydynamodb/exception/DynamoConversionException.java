package com.jojo.framework.easydynamodb.exception;

/**
 * Exception thrown when type conversion between Java types and DynamoDB AttributeValue fails.
 * 当 Java 类型与 DynamoDB AttributeValue 之间的类型转换失败时抛出的异常。
 * <p>
 * Contains the field name, source type, and target type for diagnostic purposes.
 * 包含字段名、源类型和目标类型，用于诊断。
 */
public class DynamoConversionException extends DynamoException {

    private final String fieldName;
    private final Class<?> sourceType;
    private final Class<?> targetType;

    /**
     * Constructs a new DynamoConversionException with field and type information.
     * 使用字段和类型信息构造一个新的 DynamoConversionException。
     *
     * @param fieldName  the name of the field that failed conversion / 转换失败的字段名
     * @param sourceType the source type / 源类型
     * @param targetType the target type / 目标类型
     */
    public DynamoConversionException(String fieldName, Class<?> sourceType, Class<?> targetType) {
        super(String.format("Failed to convert field '%s' from %s to %s",
                fieldName, sourceType.getName(), targetType.getName()));
        this.fieldName = fieldName;
        this.sourceType = sourceType;
        this.targetType = targetType;
    }

    /**
     * Constructs a new DynamoConversionException with field, type information, and cause.
     * 使用字段、类型信息和原因构造一个新的 DynamoConversionException。
     *
     * @param fieldName  the name of the field that failed conversion / 转换失败的字段名
     * @param sourceType the source type / 源类型
     * @param targetType the target type / 目标类型
     * @param cause      the underlying cause / 底层原因
     */
    public DynamoConversionException(String fieldName, Class<?> sourceType, Class<?> targetType, Throwable cause) {
        super(String.format("Failed to convert field '%s' from %s to %s",
                fieldName, sourceType.getName(), targetType.getName()), cause);
        this.fieldName = fieldName;
        this.sourceType = sourceType;
        this.targetType = targetType;
    }

    /**
     * Returns the name of the field that failed conversion.
     * 返回转换失败的字段名。
     *
     * @return the field name / 字段名
     */
    public String getFieldName() {
        return fieldName;
    }

    /**
     * Returns the source type of the failed conversion.
     * 返回转换失败的源类型。
     *
     * @return the source type / 源类型
     */
    public Class<?> getSourceType() {
        return sourceType;
    }

    /**
     * Returns the target type of the failed conversion.
     * 返回转换失败的目标类型。
     *
     * @return the target type / 目标类型
     */
    public Class<?> getTargetType() {
        return targetType;
    }
}
