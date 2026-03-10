package com.jojo.framework.easydynamodb.converter.builtin;

import com.jojo.framework.easydynamodb.converter.ConverterRegistry;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MapConverterTest {

    private final ConverterRegistry registry = new ConverterRegistry();
    private final MapConverter converter = new MapConverter(registry::getConverter);

    @Test
    void stringValueMap_roundTrip() {
        Map<String, String> original = Map.of("key1", "val1", "key2", "val2");
        AttributeValue av = converter.toAttributeValue(original);
        assertThat(av.hasM()).isTrue();
        Map<String, ?> result = converter.fromAttributeValue(av);
        assertThat(result.get("key1")).isEqualTo("val1");
        assertThat(result.get("key2")).isEqualTo("val2");
    }

    @Test
    void emptyMap_roundTrip() {
        Map<String, String> original = Map.of();
        AttributeValue av = converter.toAttributeValue(original);
        Map<String, ?> result = converter.fromAttributeValue(av);
        assertThat(result).isEmpty();
    }

    @Test
    void mapWithNullValue_shouldHandleNull() {
        Map<String, String> original = new LinkedHashMap<>();
        original.put("key1", "val1");
        original.put("key2", null);
        AttributeValue av = converter.toAttributeValue(original);
        Map<String, ?> result = converter.fromAttributeValue(av);
        assertThat(result.get("key1")).isEqualTo("val1");
        assertThat(result.get("key2")).isNull();
    }
}
