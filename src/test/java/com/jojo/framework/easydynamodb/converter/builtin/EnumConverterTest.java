package com.jojo.framework.easydynamodb.converter.builtin;

import com.jojo.framework.easydynamodb.exception.DynamoConversionException;
import com.jojo.framework.easydynamodb.testmodel.GameStatus;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EnumConverterTest {

    private final EnumConverter<GameStatus> converter = new EnumConverter<>(GameStatus.class);

    @Test
    void toAttributeValue_shouldUseEnumName() {
        AttributeValue av = converter.toAttributeValue(GameStatus.ACTIVE);
        assertThat(av.s()).isEqualTo("ACTIVE");
    }

    @Test
    void fromAttributeValue_shouldParseEnumName() {
        AttributeValue av = AttributeValue.builder().s("BETA").build();
        assertThat(converter.fromAttributeValue(av)).isEqualTo(GameStatus.BETA);
    }

    @Test
    void roundTrip_allValues() {
        for (GameStatus status : GameStatus.values()) {
            AttributeValue av = converter.toAttributeValue(status);
            assertThat(converter.fromAttributeValue(av)).isEqualTo(status);
        }
    }

    @Test
    void invalidEnumValue_shouldThrow() {
        AttributeValue av = AttributeValue.builder().s("NONEXISTENT").build();
        assertThatThrownBy(() -> converter.fromAttributeValue(av))
                .isInstanceOf(DynamoConversionException.class);
    }

    @Test
    void targetType_shouldReturnEnumClass() {
        assertThat(converter.targetType()).isEqualTo(GameStatus.class);
    }
}
