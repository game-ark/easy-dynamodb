package com.jojo.framework.easydynamodb.converter.builtin;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import static org.assertj.core.api.Assertions.assertThat;

class StringConverterTest {

    private final StringConverter converter = new StringConverter();

    @Test
    void toAttributeValue_shouldReturnStringType() {
        AttributeValue av = converter.toAttributeValue("hello");
        assertThat(av.s()).isEqualTo("hello");
    }

    @Test
    void fromAttributeValue_shouldReturnString() {
        AttributeValue av = AttributeValue.builder().s("world").build();
        assertThat(converter.fromAttributeValue(av)).isEqualTo("world");
    }

    @Test
    void targetType_shouldReturnStringClass() {
        assertThat(converter.targetType()).isEqualTo(String.class);
    }

    @Test
    void roundTrip_shouldPreserveValue() {
        String original = "test-value-123";
        AttributeValue av = converter.toAttributeValue(original);
        String result = converter.fromAttributeValue(av);
        assertThat(result).isEqualTo(original);
    }

    @Test
    void emptyString_shouldRoundTrip() {
        AttributeValue av = converter.toAttributeValue("");
        assertThat(converter.fromAttributeValue(av)).isEmpty();
    }
}
