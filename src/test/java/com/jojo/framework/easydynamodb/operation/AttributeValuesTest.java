package com.jojo.framework.easydynamodb.operation;

import com.jojo.framework.easydynamodb.exception.DynamoException;
import com.jojo.framework.easydynamodb.testmodel.GameStatus;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AttributeValuesTest {

    @Test
    void of_string_shouldReturnS() {
        AttributeValue av = AttributeValues.of("hello");
        assertThat(av.s()).isEqualTo("hello");
    }

    @Test
    void of_integer_shouldReturnN() {
        AttributeValue av = AttributeValues.of(42);
        assertThat(av.n()).isEqualTo("42");
    }

    @Test
    void of_long_shouldReturnN() {
        AttributeValue av = AttributeValues.of(123456789L);
        assertThat(av.n()).isEqualTo("123456789");
    }

    @Test
    void of_double_shouldReturnN() {
        AttributeValue av = AttributeValues.of(9.5);
        assertThat(av.n()).isEqualTo("9.5");
    }

    @Test
    void of_float_shouldReturnN() {
        AttributeValue av = AttributeValues.of(3.14f);
        assertThat(av.n()).isEqualTo("3.14");
    }

    @Test
    void of_bigDecimal_shouldReturnN() {
        AttributeValue av = AttributeValues.of(new BigDecimal("99.99"));
        assertThat(av.n()).isEqualTo("99.99");
    }

    @Test
    void of_short_shouldReturnN() {
        AttributeValue av = AttributeValues.of((short) 7);
        assertThat(av.n()).isEqualTo("7");
    }

    @Test
    void of_byte_shouldReturnN() {
        AttributeValue av = AttributeValues.of((byte) 3);
        assertThat(av.n()).isEqualTo("3");
    }

    @Test
    void of_boolean_shouldReturnBOOL() {
        assertThat(AttributeValues.of(true).bool()).isTrue();
        assertThat(AttributeValues.of(false).bool()).isFalse();
    }

    @Test
    void of_byteArray_shouldReturnB() {
        byte[] data = {1, 2, 3};
        AttributeValue av = AttributeValues.of(data);
        assertThat(av.b().asByteArray()).isEqualTo(data);
    }

    @Test
    void of_instant_shouldReturnS_iso8601() {
        Instant now = Instant.parse("2025-01-15T10:30:00Z");
        AttributeValue av = AttributeValues.of(now);
        assertThat(av.s()).isEqualTo("2025-01-15T10:30:00Z");
    }

    @Test
    void of_localDateTime_shouldReturnS_iso8601() {
        LocalDateTime ldt = LocalDateTime.of(2025, 1, 15, 10, 30, 0);
        AttributeValue av = AttributeValues.of(ldt);
        assertThat(av.s()).isEqualTo("2025-01-15T10:30");
    }

    @Test
    void of_enum_shouldReturnS_name() {
        AttributeValue av = AttributeValues.of(GameStatus.ACTIVE);
        assertThat(av.s()).isEqualTo("ACTIVE");
    }

    @Test
    void of_null_shouldReturnNUL() {
        AttributeValue av = AttributeValues.of(null);
        assertThat(av.nul()).isTrue();
    }

    @Test
    void of_attributeValue_shouldPassThrough() {
        AttributeValue original = AttributeValue.builder().s("pass-through").build();
        AttributeValue result = AttributeValues.of(original);
        assertThat(result).isSameAs(original);
    }

    @Test
    void of_unsupportedType_shouldThrow() {
        assertThatThrownBy(() -> AttributeValues.of(new Object()))
                .isInstanceOf(DynamoException.class)
                .hasMessageContaining("Cannot auto-convert type");
    }
}
