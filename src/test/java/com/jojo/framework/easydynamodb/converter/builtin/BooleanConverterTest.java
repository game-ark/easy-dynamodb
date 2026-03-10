package com.jojo.framework.easydynamodb.converter.builtin;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import static org.assertj.core.api.Assertions.assertThat;

class BooleanConverterTest {

    private final BooleanConverter converter = new BooleanConverter();

    @Test
    void true_roundTrip() {
        AttributeValue av = converter.toAttributeValue(true);
        assertThat(av.bool()).isTrue();
        assertThat(converter.fromAttributeValue(av)).isTrue();
    }

    @Test
    void false_roundTrip() {
        AttributeValue av = converter.toAttributeValue(false);
        assertThat(av.bool()).isFalse();
        assertThat(converter.fromAttributeValue(av)).isFalse();
    }

    @Test
    void targetType_shouldReturnBooleanClass() {
        assertThat(converter.targetType()).isEqualTo(Boolean.class);
    }
}
