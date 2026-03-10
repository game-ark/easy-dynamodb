package com.jojo.framework.easydynamodb.converter;

import com.jojo.framework.easydynamodb.converter.builtin.*;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ConverterRegistryTest {

    private final ConverterRegistry registry = new ConverterRegistry();

    @Test
    void shouldHaveBuiltinStringConverter() {
        assertThat(registry.getConverter(String.class)).isInstanceOf(StringConverter.class);
    }

    @Test
    void shouldHaveBuiltinIntegerConverter() {
        assertThat(registry.getConverter(Integer.class)).isInstanceOf(NumberConverter.class);
        assertThat(registry.getConverter(int.class)).isInstanceOf(NumberConverter.class);
    }

    @Test
    void shouldHaveBuiltinLongConverter() {
        assertThat(registry.getConverter(Long.class)).isInstanceOf(NumberConverter.class);
        assertThat(registry.getConverter(long.class)).isInstanceOf(NumberConverter.class);
    }

    @Test
    void shouldHaveBuiltinDoubleConverter() {
        assertThat(registry.getConverter(Double.class)).isInstanceOf(NumberConverter.class);
        assertThat(registry.getConverter(double.class)).isInstanceOf(NumberConverter.class);
    }

    @Test
    void shouldHaveBuiltinBooleanConverter() {
        assertThat(registry.getConverter(Boolean.class)).isInstanceOf(BooleanConverter.class);
        assertThat(registry.getConverter(boolean.class)).isInstanceOf(BooleanConverter.class);
    }

    @Test
    void shouldHaveBuiltinBinaryConverter() {
        assertThat(registry.getConverter(byte[].class)).isInstanceOf(BinaryConverter.class);
    }

    @Test
    void shouldHaveBuiltinInstantConverter() {
        assertThat(registry.getConverter(Instant.class)).isInstanceOf(InstantConverter.class);
    }

    @Test
    void shouldHaveBuiltinLocalDateTimeConverter() {
        assertThat(registry.getConverter(LocalDateTime.class)).isInstanceOf(LocalDateTimeConverter.class);
    }

    @Test
    void shouldHaveBuiltinListConverter() {
        assertThat(registry.getConverter(List.class)).isInstanceOf(ListConverter.class);
    }

    @Test
    void shouldHaveBuiltinMapConverter() {
        assertThat(registry.getConverter(Map.class)).isInstanceOf(MapConverter.class);
    }

    @Test
    void shouldHaveBuiltinSetConverter() {
        assertThat(registry.getConverter(Set.class)).isInstanceOf(SetConverter.class);
    }

    @Test
    void shouldReturnNullForUnregisteredType() {
        assertThat(registry.getConverter(StringBuilder.class)).isNull();
    }

    @Test
    void customConverter_shouldOverrideBuiltin() {
        StringConverter custom = new StringConverter();
        registry.register(String.class, custom);
        assertThat(registry.getConverter(String.class)).isSameAs(custom);
    }

    @Test
    void lookupFunction_shouldReturnConverters() {
        var lookup = registry.lookupFunction();
        assertThat(lookup.apply(String.class)).isNotNull();
        assertThat(lookup.apply(Integer.class)).isNotNull();
    }
}
