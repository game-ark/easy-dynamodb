package com.jojo.framework.easydynamodb.converter.builtin;

import com.jojo.framework.easydynamodb.exception.DynamoConversionException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InstantConverterTest {

    private final InstantConverter converter = new InstantConverter();

    @Test
    void roundTrip_shouldPreserveInstant() {
        Instant now = Instant.parse("2025-01-15T10:30:00Z");
        AttributeValue av = converter.toAttributeValue(now);
        assertThat(av.s()).isEqualTo("2025-01-15T10:30:00Z");
        assertThat(converter.fromAttributeValue(av)).isEqualTo(now);
    }

    @Test
    void invalidFormat_shouldThrow() {
        AttributeValue av = AttributeValue.builder().s("not-a-date").build();
        assertThatThrownBy(() -> converter.fromAttributeValue(av))
                .isInstanceOf(DynamoConversionException.class);
    }
}
