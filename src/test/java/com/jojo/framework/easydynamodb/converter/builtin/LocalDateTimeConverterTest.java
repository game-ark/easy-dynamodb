package com.jojo.framework.easydynamodb.converter.builtin;

import com.jojo.framework.easydynamodb.exception.DynamoConversionException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocalDateTimeConverterTest {

    private final LocalDateTimeConverter converter = new LocalDateTimeConverter();

    @Test
    void roundTrip_shouldPreserveLocalDateTime() {
        LocalDateTime dt = LocalDateTime.of(2025, 6, 15, 14, 30, 0);
        AttributeValue av = converter.toAttributeValue(dt);
        assertThat(converter.fromAttributeValue(av)).isEqualTo(dt);
    }

    @Test
    void invalidFormat_shouldThrow() {
        AttributeValue av = AttributeValue.builder().s("invalid").build();
        assertThatThrownBy(() -> converter.fromAttributeValue(av))
                .isInstanceOf(DynamoConversionException.class);
    }
}
