package com.jojo.framework.easydynamodb.exception;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DynamoConditionFailedExceptionTest {

    @Test
    void shouldExtendDynamoException() {
        DynamoConditionFailedException ex = new DynamoConditionFailedException("condition failed");
        assertThat(ex).isInstanceOf(DynamoException.class);
        assertThat(ex.getMessage()).isEqualTo("condition failed");
    }

    @Test
    void shouldPreserveCause() {
        RuntimeException cause = new RuntimeException("root cause");
        DynamoConditionFailedException ex = new DynamoConditionFailedException("failed", cause);
        assertThat(ex.getCause()).isSameAs(cause);
    }
}
