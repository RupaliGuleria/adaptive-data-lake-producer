package com.adaptivedata.ingestion.processor;

import com.adaptivedata.ingestion.config.IngestionConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DeduplicationStoreTest {

    private DeduplicationStore store;

    @BeforeEach
    void setUp() {
        store = new DeduplicationStore(new IngestionConfig());
    }

    @Test
    void unseenKey_isDuplicate_returnsFalse() {
        assertThat(store.isDuplicate("new-key")).isFalse();
    }

    @Test
    void markedKey_isDuplicate_returnsTrue() {
        store.mark("key-1");
        assertThat(store.isDuplicate("key-1")).isTrue();
    }

    @Test
    void differentKeys_independentlyTracked() {
        store.mark("key-A");

        assertThat(store.isDuplicate("key-A")).isTrue();
        assertThat(store.isDuplicate("key-B")).isFalse();
    }

    @Test
    void markCalledTwice_stillReturnsTrueOnSecondCheck() {
        store.mark("key-1");
        store.mark("key-1"); // idempotent — should not throw
        assertThat(store.isDuplicate("key-1")).isTrue();
    }

    @Test
    void multipleUniqueKeys_allTrackedIndependently() {
        store.mark("k1");
        store.mark("k2");
        store.mark("k3");

        assertThat(store.isDuplicate("k1")).isTrue();
        assertThat(store.isDuplicate("k2")).isTrue();
        assertThat(store.isDuplicate("k3")).isTrue();
        assertThat(store.isDuplicate("k4")).isFalse();
    }
}
