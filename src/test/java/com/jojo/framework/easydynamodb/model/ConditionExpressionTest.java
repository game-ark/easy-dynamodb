package com.jojo.framework.easydynamodb.model;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConditionExpressionTest {

    @Test
    void of_simpleExpression_shouldCreateWithoutNamesOrValues() {
        ConditionExpression cond = ConditionExpression.of("attribute_not_exists(userId)");

        assertThat(cond.getExpression()).isEqualTo("attribute_not_exists(userId)");
        assertThat(cond.getExpressionNames()).isEmpty();
        assertThat(cond.getExpressionValues()).isEmpty();
    }

    @Test
    void builder_withNamesAndValues_shouldBuildCorrectly() {
        ConditionExpression cond = ConditionExpression.builder()
                .expression("#coins >= :required")
                .name("#coins", "coins")
                .value(":required", 100)
                .build();

        assertThat(cond.getExpression()).isEqualTo("#coins >= :required");
        assertThat(cond.getExpressionNames()).containsEntry("#coins", "coins");
        assertThat(cond.getExpressionValues()).containsKey(":required");
        assertThat(cond.getExpressionValues().get(":required").n()).isEqualTo("100");
    }

    @Test
    void builder_withMultipleNamesAndValues_shouldMergeAll() {
        ConditionExpression cond = ConditionExpression.builder()
                .expression("#a > :x AND #b < :y")
                .name("#a", "fieldA")
                .name("#b", "fieldB")
                .value(":x", 10)
                .value(":y", 99)
                .build();

        assertThat(cond.getExpressionNames()).hasSize(2);
        assertThat(cond.getExpressionValues()).hasSize(2);
    }

    @Test
    void builder_withRawValue_shouldAcceptAttributeValue() {
        AttributeValue av = AttributeValue.builder().s("hello").build();
        ConditionExpression cond = ConditionExpression.builder()
                .expression("#f = :v")
                .name("#f", "field")
                .rawValue(":v", av)
                .build();

        assertThat(cond.getExpressionValues().get(":v").s()).isEqualTo("hello");
    }

    @Test
    void builder_withRawValues_shouldMergeMap() {
        ConditionExpression cond = ConditionExpression.builder()
                .expression("#f = :v")
                .name("#f", "field")
                .rawValues(java.util.Map.of(":v", AttributeValue.builder().n("42").build()))
                .build();

        assertThat(cond.getExpressionValues().get(":v").n()).isEqualTo("42");
    }

    @Test
    void builder_withNames_shouldMergeMap() {
        ConditionExpression cond = ConditionExpression.builder()
                .expression("#a = :v AND #b = :w")
                .names(java.util.Map.of("#a", "alpha", "#b", "beta"))
                .value(":v", "x")
                .value(":w", "y")
                .build();

        assertThat(cond.getExpressionNames()).hasSize(2);
        assertThat(cond.getExpressionNames()).containsEntry("#a", "alpha");
        assertThat(cond.getExpressionNames()).containsEntry("#b", "beta");
    }

    @Test
    void builder_nullExpression_shouldThrow() {
        assertThatThrownBy(() -> ConditionExpression.builder().build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("must not be null or empty");
    }

    @Test
    void builder_emptyExpression_shouldThrow() {
        assertThatThrownBy(() -> ConditionExpression.builder().expression("").build())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void expressionNames_shouldBeUnmodifiable() {
        ConditionExpression cond = ConditionExpression.builder()
                .expression("#f = :v")
                .name("#f", "field")
                .value(":v", 1)
                .build();

        assertThatThrownBy(() -> cond.getExpressionNames().put("#x", "y"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void expressionValues_shouldBeUnmodifiable() {
        ConditionExpression cond = ConditionExpression.builder()
                .expression("#f = :v")
                .name("#f", "field")
                .value(":v", 1)
                .build();

        assertThatThrownBy(() -> cond.getExpressionValues().put(":x", AttributeValue.builder().s("y").build()))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
