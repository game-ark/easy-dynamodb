package com.jojo.framework.easydynamodb.converter.builtin;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import static org.assertj.core.api.Assertions.assertThat;

class BinaryConverterTest {

    private final BinaryConverter converter = new BinaryConverter();

    @Test
    void roundTrip_shouldPreserveBytes() {
        byte[] original = {1, 2, 3, 4, 5};
        AttributeValue av = converter.toAttributeValue(original);
        byte[] result = converter.fromAttributeValue(av);
        assertThat(result).isEqualTo(original);
    }

    @Test
    void emptyBytes_shouldRoundTrip() {
        byte[] original = {};
        AttributeValue av = converter.toAttributeValue(original);
        assertThat(converter.fromAttributeValue(av)).isEmpty();
    }

    @Test
    void targetType_shouldReturnByteArrayClass() {
        assertThat(converter.targetType()).isEqualTo(byte[].class);
    }
}
