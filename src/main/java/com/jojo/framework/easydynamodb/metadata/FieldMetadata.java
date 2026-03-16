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
 * 持有单个实体字段的缓存元数据，包括基于 MethodHandle 的访问器和绑定的
 * AttributeConverter。为最大运行时性能而设计——所有反射工作在注册时一次性完成。
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

    /**
     * Constructs a FieldMetadata instance with all field mapping information.
     * 使用所有字段映射信息构造 FieldMetadata 实例。
     *
     * @param javaFieldName      the Java field name / Java 字段名
     * @param dynamoAttributeName the DynamoDB attribute name / DynamoDB 属性名
     * @param fieldType          the Java class of the field / 字段的 Java 类型
     * @param genericType        the generic type of the field / 字段的泛型类型
     * @param getter             the MethodHandle for reading the field / 用于读取字段的 MethodHandle
     * @param setter             the MethodHandle for writing the field / 用于写入字段的 MethodHandle
     * @param converter          the bound AttributeConverter / 绑定的 AttributeConverter
     * @param isPartitionKey     whether this field is the partition key / 此字段是否为分区键
     * @param isSortKey          whether this field is the sort key / 此字段是否为排序键
     * @param isIgnored          whether this field is ignored during mapping / 此字段在映射时是否被忽略
     */
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
     * 通过 MethodHandle 从给定实体中读取字段值。
     *
     * @param entity the entity instance to read from / 要读取的实体实例
     * @return the field value / 字段值
     * @throws DynamoConversionException if the read operation fails / 读取操作失败时抛出
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
     * 通过 MethodHandle 向给定实体写入字段值。
     *
     * @param entity the entity instance to write to / 要写入的实体实例
     * @param value  the value to set / 要设置的值
     * @throws DynamoConversionException if the write operation fails / 写入操作失败时抛出
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
     * 使用绑定的转换器将 Java 值转换为 DynamoDB AttributeValue。
     *
     * @param value the Java value to convert / 要转换的 Java 值
     * @return the corresponding DynamoDB AttributeValue / 对应的 DynamoDB AttributeValue
     * @throws DynamoConversionException if conversion fails / 转换失败时抛出
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
     * 使用绑定的转换器将 DynamoDB AttributeValue 转换回 Java 值。
     *
     * @param av the DynamoDB AttributeValue to convert / 要转换的 DynamoDB AttributeValue
     * @return the corresponding Java value / 对应的 Java 值
     * @throws DynamoConversionException if conversion fails / 转换失败时抛出
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

    /**
     * Returns the Java field name.
     * 返回 Java 字段名。
     *
     * @return the Java field name / Java 字段名
     */
    public String getJavaFieldName() {
        return javaFieldName;
    }

    /**
     * Returns the DynamoDB attribute name.
     * 返回 DynamoDB 属性名。
     *
     * @return the DynamoDB attribute name / DynamoDB 属性名
     */
    public String getDynamoAttributeName() {
        return dynamoAttributeName;
    }

    /**
     * Returns the Java class of the field.
     * 返回字段的 Java 类型。
     *
     * @return the field type / 字段类型
     */
    public Class<?> getFieldType() {
        return fieldType;
    }

    /**
     * Returns the generic type of the field (includes parameterized type info).
     * 返回字段的泛型类型（包含参数化类型信息）。
     *
     * @return the generic type / 泛型类型
     */
    public Type getGenericType() {
        return genericType;
    }

    /**
     * Returns the MethodHandle used for reading the field value.
     * 返回用于读取字段值的 MethodHandle。
     *
     * @return the getter MethodHandle / getter 的 MethodHandle
     */
    public MethodHandle getGetter() {
        return getter;
    }

    /**
     * Returns the MethodHandle used for writing the field value.
     * 返回用于写入字段值的 MethodHandle。
     *
     * @return the setter MethodHandle / setter 的 MethodHandle
     */
    public MethodHandle getSetter() {
        return setter;
    }

    /**
     * Returns the bound AttributeConverter for this field.
     * 返回此字段绑定的 AttributeConverter。
     *
     * @return the attribute converter / 属性转换器
     */
    public AttributeConverter<?> getConverter() {
        return converter;
    }

    /**
     * Returns whether this field is the partition key.
     * 返回此字段是否为分区键。
     *
     * @return true if this field is the partition key / 如果此字段是分区键则返回 true
     */
    public boolean isPartitionKey() {
        return isPartitionKey;
    }

    /**
     * Returns whether this field is the sort key.
     * 返回此字段是否为排序键。
     *
     * @return true if this field is the sort key / 如果此字段是排序键则返回 true
     */
    public boolean isSortKey() {
        return isSortKey;
    }

    /**
     * Returns whether this field is ignored during mapping.
     * 返回此字段在映射时是否被忽略。
     *
     * @return true if this field is ignored / 如果此字段被忽略则返回 true
     */
    public boolean isIgnored() {
        return isIgnored;
    }
}
