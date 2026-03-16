package com.jojo.framework.easydynamodb.exception;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DynamoTransactionExceptionTest {

    @Test
    void shouldExtendDynamoException() {
        DynamoTransactionException ex = new DynamoTransactionException("tx failed");
        assertThat(ex).isInstanceOf(DynamoException.class);
        assertThat(ex.getMessage()).isEqualTo("tx failed");
    }

    @Test
    void shouldPreserveCause() {
        RuntimeException cause = new RuntimeException("root");
        DynamoTransactionException ex = new DynamoTransactionException("failed", cause);
        assertThat(ex.getCause()).isSameAs(cause);
    }

    @Test
    void shouldStoreCancellationReasons() {
        List<String> reasons = List.of("ConditionalCheckFailed: item 0", "None: item 1");
        DynamoTransactionException ex = new DynamoTransactionException("cancelled", reasons, new RuntimeException());

        assertThat(ex.getCancellationReasons()).hasSize(2);
        assertThat(ex.getCancellationReasons().get(0)).contains("ConditionalCheckFailed");
    }

    @Test
    void cancellationReasons_shouldBeUnmodifiable() {
        List<String> reasons = List.of("reason1");
        DynamoTransactionException ex = new DynamoTransactionException("cancelled", reasons, new RuntimeException());

        assertThatThrownBy(() -> ex.getCancellationReasons().add("new"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void noCancellationReasons_shouldReturnEmptyList() {
        DynamoTransactionException ex = new DynamoTransactionException("failed");
        assertThat(ex.getCancellationReasons()).isEmpty();
    }

    @Test
    void nullCancellationReasons_shouldReturnEmptyList() {
        DynamoTransactionException ex = new DynamoTransactionException("failed", null, new RuntimeException());
        assertThat(ex.getCancellationReasons()).isEmpty();
    }
}
