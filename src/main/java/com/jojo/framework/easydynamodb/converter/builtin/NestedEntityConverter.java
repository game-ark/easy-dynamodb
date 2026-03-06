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
 *
 * @param <T> the entity type
 */
public class NestedEntityConverter<T> implements AttributeConverter<T> {

    private final Class<T> entityClass;
    private final Function<T, Map<String, AttributeValue>> toMapFunction;
    private final Function<Map<String, AttributeValue>, T> fromMapFunction;

    /**
     * Creates a NestedEntityConverter.
     *
     * @param entityClass    the entity class
     * @param toMapFunction  converts entity → AttributeValue map (using EntityMetadata)
     * @param fromMapFunction converts AttributeValue map → entity (using EntityMetadata)
     */
    public NestedEntityConverter(Class<T> entityClass,
                                  Function<T, Map<String, AttributeValue>> toMapFunction,
                                  Function<Map<String, AttributeValue>, T> fromMapFunction) {
        this.entityClass = entityClass;
        this.toMapFunction = toMapFunction;
        this.fromMapFunction = fromMapFunction;
    }

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

    @Override
    public Class<T> targetType() {
        return entityClass;
    }
}
