package com.jojo.framework.easydynamodb.converter.builtin;

import com.jojo.framework.easydynamodb.exception.DynamoConversionException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NumberConverterTest {

    @Test
    void integerConverter_roundTrip() {
        NumberConverter converter = new NumberConverter(Integer.class);
        AttributeValue av = converter.toAttributeValue(42);
        assertThat(av.n()).isEqualTo("42");
        assertThat(converter.fromAttributeValue(av).intValue()).isEqualTo(42);
    }

    @Test
    void longConverter_roundTrip() {
        NumberConverter converter = new NumberConverter(Long.class);
        AttributeValue av = converter.toAttributeValue(123456789L);
        assertThat(converter.fromAttributeValue(av).longValue()).isEqualTo(123456789L);
    }

    @Test
    void doubleConverter_roundTrip() {
        NumberConverter converter = new NumberConverter(Double.class);
        AttributeValue av = converter.toAttributeValue(3.14);
        assertThat(converter.fromAttributeValue(av).doubleValue()).isEqualTo(3.14);
    }

    @Test
    void floatConverter_roundTrip() {
        NumberConverter converter = new NumberConverter(Float.class);
        AttributeValue av = converter.toAttributeValue(2.5f);
        assertThat(converter.fromAttributeValue(av).floatValue()).isEqualTo(2.5f);
    }

    @Test
    void bigDecimalConverter_roundTrip() {
        NumberConverter converter = new NumberConverter(BigDecimal.class);
        BigDecimal value = new BigDecimal("99999999999999.99");
        AttributeValue av = converter.toAttributeValue(value);
        assertThat(converter.fromAttributeValue(av)).isEqualTo(value);
    }

    @Test
    void invalidNumber_shouldThrow() {
        NumberConverter converter = new NumberConverter(Integer.class);
        AttributeValue av = AttributeValue.builder().n("not-a-number").build();
        assertThatThrownBy(() -> converter.fromAttributeValue(av))
                .isInstanceOf(DynamoConversionException.class);
    }
}
