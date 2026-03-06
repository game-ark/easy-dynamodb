package com.jojo.framework.easydynamodb.metadata;

import com.jojo.framework.easydynamodb.annotation.DynamoConverter;
import com.jojo.framework.easydynamodb.annotation.DynamoTable;
import com.jojo.framework.easydynamodb.converter.AttributeConverter;
import com.jojo.framework.easydynamodb.converter.ConverterRegistry;
import com.jojo.framework.easydynamodb.converter.builtin.SetConverter;
import com.jojo.framework.easydynamodb.exception.DynamoConfigException;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbIgnore;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondaryPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondarySortKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry that parses entity class annotations and caches {@link EntityMetadata}
 * for each registered class. All reflection and MethodHandle resolution happens
 * once at registration time so that runtime operations incur zero overhead.
 * <p>
 * Uses AWS Enhanced Client annotations ({@code @DynamoDbPartitionKey},
 * {@code @DynamoDbSortKey}, {@code @DynamoDbAttribute}, {@code @DynamoDbIgnore})
 * on getter methods for field-level mapping, and either {@code @DynamoTable} or
 * {@code @DynamoDbBean} as class-level entity markers.
 */
public class MetadataRegistry {

    private final Map<Class<?>, EntityMetadata> cache = new ConcurrentHashMap<>();
    private final ConverterRegistry converterRegistry;
    private final String tablePrefix;
    private final TableNameResolver tableNameResolver;

    public MetadataRegistry(ConverterRegistry converterRegistry, String tablePrefix) {
        this(converterRegistry, tablePrefix, new TableNameResolver());
    }

    public MetadataRegistry(ConverterRegistry converterRegistry, String tablePrefix, TableNameResolver tableNameResolver) {
        this.converterRegistry = converterRegistry;
        this.tablePrefix = tablePrefix == null ? "" : tablePrefix;
        this.tableNameResolver = tableNameResolver != null ? tableNameResolver : new TableNameResolver();
    }

    public void register(Class<?> entityClass) {
        cache.computeIfAbsent(entityClass, this::parseEntityClass);
    }

    public EntityMetadata getMetadata(Class<?> entityClass) {
        EntityMetadata metadata = cache.get(entityClass);
        if (metadata == null) {
            throw new DynamoConfigException(
                    "Entity class " + entityClass.getName() + " is not registered. "
                            + "Call register() before using this class.");
        }
        return metadata;
    }

    public boolean isRegistered(Class<?> entityClass) {
        return cache.containsKey(entityClass);
    }

    // ---- Internal parsing ----

    private EntityMetadata parseEntityClass(Class<?> entityClass) {
        String rawTableName = resolveTableName(entityClass);
        String tableName = tableNameResolver.resolve(rawTableName, tablePrefix);

        List<Field> allFields = collectAllFields(entityClass);

        List<FieldMetadata> fields = new ArrayList<>();
        Map<String, FieldMetadata> fieldByAttributeName = new LinkedHashMap<>();
        FieldMetadata partitionKey = null;
        FieldMetadata sortKey = null;

        // GSI: indexName -> {pk: FieldMetadata, sk: FieldMetadata}
        Map<String, FieldMetadata> gsiPartitionKeys = new LinkedHashMap<>();
        Map<String, FieldMetadata> gsiSortKeys = new LinkedHashMap<>();

        MethodHandles.Lookup lookup = MethodHandles.lookup();

        for (Field field : allFields) {
            int modifiers = field.getModifiers();
            if (Modifier.isStatic(modifiers) || Modifier.isTransient(modifiers)) {
                continue;
            }

            // Check @DynamoDbIgnore on getter
            Method getter = findGetterMethod(field, entityClass);
            if (getter != null && getter.isAnnotationPresent(DynamoDbIgnore.class)) {
                continue;
            }

            FieldMetadata fm = parseField(field, entityClass, getter, lookup);
            fields.add(fm);
            fieldByAttributeName.put(fm.getDynamoAttributeName(), fm);

            if (fm.isPartitionKey()) {
                if (partitionKey != null) {
                    throw new DynamoConfigException(
                            "Entity class " + entityClass.getName()
                                    + " has multiple @DynamoDbPartitionKey fields: "
                                    + partitionKey.getJavaFieldName() + " and " + fm.getJavaFieldName());
                }
                partitionKey = fm;
            }
            if (fm.isSortKey()) {
                sortKey = fm;
            }

            // Collect GSI annotations from getter
            if (getter != null) {
                DynamoDbSecondaryPartitionKey gsiPk = getter.getAnnotation(DynamoDbSecondaryPartitionKey.class);
                if (gsiPk != null) {
                    for (String indexName : gsiPk.indexNames()) {
                        gsiPartitionKeys.put(indexName, fm);
                    }
                }
                DynamoDbSecondarySortKey gsiSk = getter.getAnnotation(DynamoDbSecondarySortKey.class);
                if (gsiSk != null) {
                    for (String indexName : gsiSk.indexNames()) {
                        gsiSortKeys.put(indexName, fm);
                    }
                }
            }
        }

        if (partitionKey == null) {
            throw new DynamoConfigException(
                    "Entity class " + entityClass.getName()
                            + " must have exactly one getter annotated with @DynamoDbPartitionKey");
        }

        // Build GSI metadata list
        List<GsiMetadata> gsiList = new ArrayList<>();
        Set<String> allGsiNames = new LinkedHashSet<>();
        allGsiNames.addAll(gsiPartitionKeys.keySet());
        allGsiNames.addAll(gsiSortKeys.keySet());
        for (String indexName : allGsiNames) {
            FieldMetadata gsiPk = gsiPartitionKeys.get(indexName);
            FieldMetadata gsiSk = gsiSortKeys.get(indexName);
            if (gsiPk == null) {
                throw new DynamoConfigException(
                        "GSI '" + indexName + "' on entity " + entityClass.getName()
                                + " has a sort key but no partition key. "
                                + "A GSI must have a @DynamoDbSecondaryPartitionKey.");
            }
            gsiList.add(new GsiMetadata(indexName, gsiPk, gsiSk));
        }

        return new EntityMetadata(entityClass, tableName, partitionKey, sortKey, fields, fieldByAttributeName, gsiList);
    }

    private String resolveTableName(Class<?> entityClass) {
        // Priority 1: @DynamoTable with explicit value
        DynamoTable tableAnnotation = entityClass.getAnnotation(DynamoTable.class);
        if (tableAnnotation != null && !tableAnnotation.value().isEmpty()) {
            return tableAnnotation.value();
        }
        // @DynamoTable (empty) or @DynamoDbBean or no annotation → class name
        return entityClass.getSimpleName();
    }

    private List<Field> collectAllFields(Class<?> clazz) {
        List<Field> fields = new ArrayList<>();
        Class<?> current = clazz;
        while (current != null && current != Object.class) {
            Collections.addAll(fields, current.getDeclaredFields());
            current = current.getSuperclass();
        }
        return fields;
    }

    private FieldMetadata parseField(Field field, Class<?> entityClass, Method getter, MethodHandles.Lookup lookup) {
        String javaFieldName = field.getName();

        // Resolve attribute name: @DynamoDbAttribute on getter > field name
        String attributeName = javaFieldName;
        if (getter != null) {
            DynamoDbAttribute attrAnnotation = getter.getAnnotation(DynamoDbAttribute.class);
            if (attrAnnotation != null && !attrAnnotation.value().isEmpty()) {
                attributeName = attrAnnotation.value();
            }
        }

        Class<?> fieldType = field.getType();
        Type genericType = field.getGenericType();

        MethodHandle getterHandle = resolveGetter(field, entityClass, lookup);
        MethodHandle setterHandle = resolveSetter(field, entityClass, lookup);

        // Check key annotations on getter
        boolean isPartitionKey = getter != null && getter.isAnnotationPresent(DynamoDbPartitionKey.class);
        boolean isSortKey = getter != null && getter.isAnnotationPresent(DynamoDbSortKey.class);

        AttributeConverter<?> converter = resolveConverter(field, fieldType, genericType);

        return new FieldMetadata(
                javaFieldName, attributeName, fieldType, genericType,
                getterHandle, setterHandle, converter, isPartitionKey, isSortKey, false);
    }

    private Method findGetterMethod(Field field, Class<?> entityClass) {
        String fieldName = field.getName();
        String capitalized = Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);

        try {
            return entityClass.getMethod("get" + capitalized);
        } catch (NoSuchMethodException ignored) {
        }

        if (field.getType() == boolean.class || field.getType() == Boolean.class) {
            try {
                return entityClass.getMethod("is" + capitalized);
            } catch (NoSuchMethodException ignored) {
            }
        }

        return null;
    }

    private MethodHandle resolveGetter(Field field, Class<?> entityClass, MethodHandles.Lookup lookup) {
        String fieldName = field.getName();
        Class<?> fieldType = field.getType();
        String capitalized = Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);

        try {
            return lookup.findVirtual(entityClass, "get" + capitalized, MethodType.methodType(fieldType));
        } catch (NoSuchMethodException | IllegalAccessException ignored) {
        }

        if (fieldType == boolean.class || fieldType == Boolean.class) {
            try {
                return lookup.findVirtual(entityClass, "is" + capitalized, MethodType.methodType(fieldType));
            } catch (NoSuchMethodException | IllegalAccessException ignored) {
            }
        }

        try {
            field.setAccessible(true);
            return lookup.unreflectGetter(field);
        } catch (IllegalAccessException e) {
            throw new DynamoConfigException(
                    "Cannot access field '" + fieldName + "' on " + entityClass.getName()
                            + ". Ensure the field or its getter is accessible.");
        }
    }

    private MethodHandle resolveSetter(Field field, Class<?> entityClass, MethodHandles.Lookup lookup) {
        String fieldName = field.getName();
        Class<?> fieldType = field.getType();
        String capitalized = Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);

        try {
            return lookup.findVirtual(entityClass, "set" + capitalized,
                    MethodType.methodType(void.class, fieldType));
        } catch (NoSuchMethodException | IllegalAccessException ignored) {
        }

        try {
            field.setAccessible(true);
            return lookup.unreflectSetter(field);
        } catch (IllegalAccessException e) {
            throw new DynamoConfigException(
                    "Cannot access field '" + fieldName + "' on " + entityClass.getName()
                            + ". Ensure the field or its setter is accessible.");
        }
    }

    private AttributeConverter<?> resolveConverter(Field field, Class<?> fieldType, Type genericType) {
        DynamoConverter converterAnnotation = field.getAnnotation(DynamoConverter.class);
        if (converterAnnotation != null) {
            return instantiateConverter(converterAnnotation.value(), field.getName());
        }

        if (Set.class.isAssignableFrom(fieldType)) {
            return resolveSetConverter(genericType, field.getName());
        }

        AttributeConverter<?> converter = converterRegistry.getConverter(fieldType);
        if (converter != null) {
            return converter;
        }

        throw new DynamoConfigException(
                "No converter found for field '" + field.getName()
                        + "' of type " + fieldType.getName()
                        + ". Register a custom converter or annotate with @DynamoConverter.");
    }

    private AttributeConverter<?> instantiateConverter(Class<? extends AttributeConverter<?>> converterClass,
                                                        String fieldName) {
        try {
            return converterClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new DynamoConfigException(
                    "Failed to instantiate custom converter " + converterClass.getName()
                            + " for field '" + fieldName + "': " + e.getMessage());
        }
    }

    private AttributeConverter<?> resolveSetConverter(Type genericType, String fieldName) {
        if (genericType instanceof ParameterizedType pt) {
            Type[] typeArgs = pt.getActualTypeArguments();
            if (typeArgs.length > 0) {
                Type elementType = typeArgs[0];
                if (elementType instanceof Class<?> elementClass) {
                    if (isNumericType(elementClass)) {
                        return new SetConverter(SetConverter.SetType.NUMBER_SET);
                    }
                }
            }
        }
        return new SetConverter(SetConverter.SetType.STRING_SET);
    }

    private boolean isNumericType(Class<?> type) {
        return type == Integer.class || type == Long.class
                || type == Double.class || type == Float.class
                || type == java.math.BigDecimal.class
                || type == Short.class || type == Byte.class;
    }
}
