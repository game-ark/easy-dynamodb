package com.jojo.framework.easydynamodb.annotation;

import com.jojo.framework.easydynamodb.converter.AttributeConverter;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies a custom AttributeConverter for a field.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface DynamoConverter {
    Class<? extends AttributeConverter<?>> value();
}
