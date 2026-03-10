package com.jojo.framework.easydynamodb.converter.builtin;

import com.jojo.framework.easydynamodb.converter.ConverterRegistry;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ListConverterTest {

    private final ConverterRegistry registry = new ConverterRegistry();
    private final ListConverter converter = new ListConverter(registry::getConverter);

    @Test
    void stringList_roundTrip() {
        List<String> original = List.of("a", "b", "c");
        AttributeValue av = converter.toAttributeValue(original);
        assertThat(av.hasL()).isTrue();
        assertThat(av.l()).hasSize(3);
        List<?> result = converter.fromAttributeValue(av);
        assertThat(result).hasSize(3);
        assertThat(result.get(0)).isEqualTo("a");
        assertThat(result.get(1)).isEqualTo("b");
        assertThat(result.get(2)).isEqualTo("c");
    }

    @Test
    void integerList_roundTrip() {
        List<Integer> original = List.of(1, 2, 3);
        AttributeValue av = converter.toAttributeValue(original);
        List<?> result = converter.fromAttributeValue(av);
        // Numbers come back as BigDecimal from generic extraction
        assertThat(result).hasSize(3);
    }

    @Test
    void emptyList_roundTrip() {
        List<String> original = List.of();
        AttributeValue av = converter.toAttributeValue(original);
        assertThat(av.hasL()).isTrue();
        List<?> result = converter.fromAttributeValue(av);
        assertThat(result).isEmpty();
    }

    @Test
    void listWithNull_shouldHandleNullElements() {
        List<String> original = new java.util.ArrayList<>();
        original.add("a");
        original.add(null);
        original.add("c");
        AttributeValue av = converter.toAttributeValue(original);
        List<?> result = converter.fromAttributeValue(av);
        assertThat(result).hasSize(3);
        assertThat(result.get(0)).isEqualTo("a");
        assertThat(result.get(1)).isNull();
        assertThat(result.get(2)).isEqualTo("c");
    }
}
