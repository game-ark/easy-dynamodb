package com.jojo.framework.easydynamodb.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KeyPairTest {

    @Test
    void fullConstructor_shouldHoldBothKeys() {
        KeyPair kp = new KeyPair("pk-value", "sk-value");
        assertThat(kp.partitionKey()).isEqualTo("pk-value");
        assertThat(kp.sortKey()).isEqualTo("sk-value");
    }

    @Test
    void pkOnlyConstructor_shouldHaveNullSortKey() {
        KeyPair kp = new KeyPair("pk-value");
        assertThat(kp.partitionKey()).isEqualTo("pk-value");
        assertThat(kp.sortKey()).isNull();
    }

    @Test
    void equality_shouldWorkCorrectly() {
        KeyPair a = new KeyPair("pk", "sk");
        KeyPair b = new KeyPair("pk", "sk");
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void inequality_shouldWorkCorrectly() {
        KeyPair a = new KeyPair("pk1", "sk");
        KeyPair b = new KeyPair("pk2", "sk");
        assertThat(a).isNotEqualTo(b);
    }
}
