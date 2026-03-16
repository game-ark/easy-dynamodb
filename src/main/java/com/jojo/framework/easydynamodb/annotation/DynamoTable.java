package com.jojo.framework.easydynamodb.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a Java class as a DynamoDB table entity.
 * 将一个 Java 类标记为 DynamoDB 表实体。
 * <p>
 * When value is empty, the class name is used as the table name.
 * 当 value 为空时，使用类名作为表名。
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface DynamoTable {

    /**
     * The DynamoDB table name. If empty, the class name is used.
     * DynamoDB 表名。如果为空，则使用类名。
     *
     * @return the table name / 表名
     */
    String value() default "";
}
