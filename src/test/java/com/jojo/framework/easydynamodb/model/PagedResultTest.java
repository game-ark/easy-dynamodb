package com.jojo.framework.easydynamodb.model;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PagedResultTest {

    @Test
    void hasMorePages_withLastEvaluatedKey_shouldReturnTrue() {
        var result = new PagedResult<>(List.of("a"),
                Map.of("pk", AttributeValue.builder().s("x").build()));
        assertThat(result.hasMorePages()).isTrue();
    }

    @Test
    void hasMorePages_withNullKey_shouldReturnFalse() {
        var result = new PagedResult<>(List.of("a"), null);
        assertThat(result.hasMorePages()).isFalse();
    }

    @Test
    void hasMorePages_withEmptyKey_shouldReturnFalse() {
        var result = new PagedResult<>(List.of("a"), Map.of());
        assertThat(result.hasMorePages()).isFalse();
    }

    @Test
    void items_shouldReturnProvidedList() {
        var items = List.of("a", "b", "c");
        var result = new PagedResult<>(items, null);
        assertThat(result.items()).isEqualTo(items);
    }
}
