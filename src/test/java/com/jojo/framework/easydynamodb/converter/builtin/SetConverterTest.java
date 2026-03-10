package com.jojo.framework.easydynamodb.converter.builtin;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.math.BigDecimal;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class SetConverterTest {

    @Test
    void stringSet_roundTrip() {
        SetConverter converter = new SetConverter(SetConverter.SetType.STRING_SET);
        Set<String> original = new LinkedHashSet<>(Set.of("a", "b", "c"));
        AttributeValue av = converter.toAttributeValue(original);
        assertThat(av.hasSs()).isTrue();
        Set<?> result = converter.fromAttributeValue(av);
        assertThat(result).hasSize(3);
        assertThat(result.contains("a")).isTrue();
        assertThat(result.contains("b")).isTrue();
        assertThat(result.contains("c")).isTrue();
    }

    @Test
    void numberSet_roundTrip() {
        SetConverter converter = new SetConverter(SetConverter.SetType.NUMBER_SET);
        Set<Integer> original = new LinkedHashSet<>(Set.of(1, 2, 3));
        AttributeValue av = converter.toAttributeValue(original);
        assertThat(av.hasNs()).isTrue();
        Set<?> result = converter.fromAttributeValue(av);
        assertThat(result).hasSize(3);
        assertThat(result.contains(new BigDecimal("1"))).isTrue();
        assertThat(result.contains(new BigDecimal("2"))).isTrue();
        assertThat(result.contains(new BigDecimal("3"))).isTrue();
    }

    @Test
    void emptySet_shouldStoreAsNull() {
        SetConverter converter = new SetConverter(SetConverter.SetType.STRING_SET);
        Set<String> empty = new LinkedHashSet<>();
        AttributeValue av = converter.toAttributeValue(empty);
        assertThat(av.nul()).isTrue();
    }

    @Test
    void emptySet_fromNull_shouldReturnEmptySet() {
        SetConverter converter = new SetConverter(SetConverter.SetType.STRING_SET);
        AttributeValue av = AttributeValue.builder().nul(true).build();
        Set<?> result = converter.fromAttributeValue(av);
        assertThat(result).isEmpty();
    }
}
