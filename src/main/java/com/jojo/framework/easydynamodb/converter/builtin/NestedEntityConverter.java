package com.jojo.framework.easydynamodb.converter.builtin;

import com.jojo.framework.easydynamodb.converter.AttributeConverter;
import com.jojo.framework.easydynamodb.exception.DynamoConversionException;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Map;
import java.util.function.Function;

/**
 * Converts nested Entity objects ↔ DynamoDB M (Map) type.
 * <p>
 * Uses functional interfaces for entity-to-map and map-to-entity conversion,
 * which will be wired to EntityMetadata once it is implemented (Task 4).
 * 转换嵌套实体对象 ↔ DynamoDB M（映射）类型。
 * <p>
 * 使用函数式接口进行实体到映射和映射到实体的转换，
 * 将在 EntityMetadata 实现后（任务 4）进行连接。
 *
 * @param <T> the entity type / 实体类型
 */
public class NestedEntityConverter<T> implements AttributeConverter<T> {

    private final Class<T> entityClass;
    private final Function<T, Map<String, AttributeValue>> toMapFunction;
    private final Function<Map<String, AttributeValue>, T> fromMapFunction;

    /**
     * Creates a NestedEntityConverter.
     * 创建一个 NestedEntityConverter。
     *
     * @param entityClass    the entity class / 实体类
     * @param toMapFunction  converts entity → AttributeValue map (using EntityMetadata) / 将实体转换为 AttributeValue 映射（使用 EntityMetadata）
     * @param fromMapFunction converts AttributeValue map → entity (using EntityMetadata) / 将 AttributeValue 映射转换为实体（使用 EntityMetadata）
     */
    public NestedEntityConverter(Class<T> entityClass,
                                  Function<T, Map<String, AttributeValue>> toMapFunction,
                                  Function<Map<String, AttributeValue>, T> fromMapFunction) {
        this.entityClass = entityClass;
        this.toMapFunction = toMapFunction;
        this.fromMapFunction = fromMapFunction;
    }

    /**
     * Converts an entity to a DynamoDB M (Map) AttributeValue.
     * 将实体转换为 DynamoDB M（映射）AttributeValue。
     *
     * @param value the entity to convert / 要转换的实体
     * @return the DynamoDB M AttributeValue / DynamoDB M 类型的 AttributeValue
     */
    @Override
    public AttributeValue toAttributeValue(T value) {
        try {
            Map<String, AttributeValue> map = toMapFunction.apply(value);
            return AttributeValue.builder().m(map).build();
        } catch (DynamoConversionException e) {
            throw e;
        } catch (Exception e) {
            throw new DynamoConversionException("nested-entity",
                    entityClass, AttributeValue.class, e);
        }
    }

    /**
     * Converts a DynamoDB M (Map) AttributeValue back to an entity.
     * 将 DynamoDB M（映射）AttributeValue 转换回实体。
     *
     * @param attributeValue the DynamoDB AttributeValue to convert / 要转换的 DynamoDB AttributeValue
     * @return the reconstructed entity / 重建的实体
     */
    @Override
    public T fromAttributeValue(AttributeValue attributeValue) {
        try {
            Map<String, AttributeValue> map = attributeValue.m();
            return fromMapFunction.apply(map);
        } catch (DynamoConversionException e) {
            throw e;
        } catch (Exception e) {
            throw new DynamoConversionException("nested-entity",
                    AttributeValue.class, entityClass, e);
        }
    }

    /**
     * Returns the target entity class this converter handles.
     * 返回此转换器处理的目标实体类。
     *
     * @return the entity class / 实体类
     */
    @Override
    public Class<T> targetType() {
        return entityClass;
    }
}
