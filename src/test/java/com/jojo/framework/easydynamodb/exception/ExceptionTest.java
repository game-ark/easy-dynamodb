package com.jojo.framework.easydynamodb.exception;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ExceptionTest {

    @Test
    void dynamoException_shouldHoldMessage() {
        DynamoException ex = new DynamoException("test error");
        assertThat(ex.getMessage()).isEqualTo("test error");
    }

    @Test
    void dynamoException_shouldHoldCause() {
        RuntimeException cause = new RuntimeException("root");
        DynamoException ex = new DynamoException("wrapped", cause);
        assertThat(ex.getCause()).isSameAs(cause);
    }

    @Test
    void dynamoConfigException_shouldExtendDynamoException() {
        DynamoConfigException ex = new DynamoConfigException("config error");
        assertThat(ex).isInstanceOf(DynamoException.class);
        assertThat(ex.getMessage()).isEqualTo("config error");
    }

    @Test
    void dynamoConversionException_shouldHoldFieldInfo() {
        DynamoConversionException ex = new DynamoConversionException("myField", String.class, Integer.class);
        assertThat(ex.getFieldName()).isEqualTo("myField");
        assertThat(ex.getSourceType()).isEqualTo(String.class);
        assertThat(ex.getTargetType()).isEqualTo(Integer.class);
        assertThat(ex.getMessage()).contains("myField");
    }

    @Test
    void dynamoConversionException_withCause() {
        RuntimeException cause = new RuntimeException("parse error");
        DynamoConversionException ex = new DynamoConversionException("field", String.class, Integer.class, cause);
        assertThat(ex.getCause()).isSameAs(cause);
    }

    @Test
    void dynamoBatchException_shouldHoldFailures() {
        var failures = List.of(
                new DynamoBatchException.BatchFailure("key1", "error1"),
                new DynamoBatchException.BatchFailure("key2", "error2")
        );
        DynamoBatchException ex = new DynamoBatchException(failures);
        assertThat(ex.getFailures()).hasSize(2);
        assertThat(ex.getMessage()).contains("2 error(s)");
    }

    @Test
    void batchFailure_shouldHoldKeyAndMessage() {
        var failure = new DynamoBatchException.BatchFailure("pk=123", "throttled");
        assertThat(failure.key()).isEqualTo("pk=123");
        assertThat(failure.errorMessage()).isEqualTo("throttled");
    }
}
