package com.jojo.framework.easydynamodb.annotation;

import com.jojo.framework.easydynamodb.converter.AttributeConverter;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies a custom AttributeConverter for a field.
 * 为字段指定自定义的 AttributeConverter。
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface DynamoConverter {

    /**
     * The AttributeConverter class to use for this field.
     * 用于此字段的 AttributeConverter 类。
     *
     * @return the converter class / 转换器类
     */
    Class<? extends AttributeConverter<?>> value();
}
