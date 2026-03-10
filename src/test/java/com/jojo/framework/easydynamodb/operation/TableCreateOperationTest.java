package com.jojo.framework.easydynamodb.operation;

import com.jojo.framework.easydynamodb.exception.DynamoException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableCreateOperationTest {

    @Test
    void toDynamoScalarType_string_shouldReturnS() {
        assertThat(TableCreateOperation.toDynamoScalarType(String.class))
                .isEqualTo(ScalarAttributeType.S);
    }

    @Test
    void toDynamoScalarType_integer_shouldReturnN() {
        assertThat(TableCreateOperation.toDynamoScalarType(Integer.class))
                .isEqualTo(ScalarAttributeType.N);
    }

    @Test
    void toDynamoScalarType_long_shouldReturnN() {
        assertThat(TableCreateOperation.toDynamoScalarType(Long.class))
                .isEqualTo(ScalarAttributeType.N);
    }

    @Test
    void toDynamoScalarType_intPrimitive_shouldReturnN() {
        assertThat(TableCreateOperation.toDynamoScalarType(int.class))
                .isEqualTo(ScalarAttributeType.N);
    }

    @Test
    void toDynamoScalarType_bigDecimal_shouldReturnN() {
        assertThat(TableCreateOperation.toDynamoScalarType(BigDecimal.class))
                .isEqualTo(ScalarAttributeType.N);
    }

    @Test
    void toDynamoScalarType_byteArray_shouldReturnB() {
        assertThat(TableCreateOperation.toDynamoScalarType(byte[].class))
                .isEqualTo(ScalarAttributeType.B);
    }

    @Test
    void toDynamoScalarType_unsupported_shouldThrow() {
        assertThatThrownBy(() -> TableCreateOperation.toDynamoScalarType(Boolean.class))
                .isInstanceOf(DynamoException.class)
                .hasMessageContaining("Unsupported key type");
    }
}
