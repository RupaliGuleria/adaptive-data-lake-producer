package com.adaptivedata.ingestion.coordinator;

import com.adaptivedata.ingestion.config.IngestionConfig;
import com.adaptivedata.ingestion.model.ProcessedEvent;
import com.adaptivedata.ingestion.model.TradeDocMessage;
import com.adaptivedata.ingestion.routing.DlqPublisher;
import com.adaptivedata.ingestion.staging.StagingBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class BatchCoordinatorTest {

    @Mock private StagingBuffer stagingBuffer;
    @Mock private DlqPublisher dlqPublisher;

    private BatchCoordinator coordinator;
    private IngestionConfig config;

    @BeforeEach
    void setUp() {
        config = new IngestionConfig();
        coordinator = new BatchCoordinator(stagingBuffer, dlqPublisher, config);
    }

    // -----------------------------------------------------------------------
    // Registration
    // -----------------------------------------------------------------------

    @Test
    void register_batchIsRegistered() {
        coordinator.register(tradeDoc("G1", 2, List.of("T1", "T2")));
        assertThat(coordinator.isRegistered("G1")).isTrue();
    }

    @Test
    void isRegistered_unknownGroup_returnsFalse() {
        assertThat(coordinator.isRegistered("unknown")).isFalse();
    }

    // -----------------------------------------------------------------------
    // Success path: all events received → staged and removed from coordinator
    // -----------------------------------------------------------------------

    @Test
    void allEventsReceived_batchMarkedSuccess_flushedToStaging() {
        coordinator.register(tradeDoc("G1", 2, List.of("T1", "T2")));

        coordinator.recordProcessed(event("G1", "T1", "k1"));
        coordinator.recordProcessed(event("G1", "T2", "k2"));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<ProcessedEvent>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(stagingBuffer).addAll(captor.capture());
        assertThat(captor.getValue()).hasSize(2);

        // Batch removed from coordinator after SUCCESS
        assertThat(coordinator.isRegistered("G1")).isFalse();
    }

    @Test
    void partialEvents_batchRemainsRegistered_stagingNotCalled() {
        coordinator.register(tradeDoc("G1", 3, List.of("T1", "T2", "T3")));

        coordinator.recordProcessed(event("G1", "T1", "k1"));
        coordinator.recordProcessed(event("G1", "T2", "k2"));
        // T3 never arrives

        verify(stagingBuffer, never()).addAll(any());
        assertThat(coordinator.isRegistered("G1")).isTrue();
    }

    // -----------------------------------------------------------------------
    // Deduplication: count only reflects unique trade IDs reaching coordinator
    // -----------------------------------------------------------------------

    @Test
    void countMatchesAfterDedup_successTriggeredExactlyOnce() {
        coordinator.register(tradeDoc("G1", 2, List.of("T1", "T2")));

        // Simulate BatchProcessor already deduplicating — only unique events reach coordinator
        coordinator.recordProcessed(event("G1", "T1", "k1"));
        coordinator.recordProcessed(event("G1", "T2", "k2"));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<ProcessedEvent>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(stagingBuffer).addAll(captor.capture());

        // Exactly 2 unique events staged, success triggered exactly once
        assertThat(captor.getValue()).hasSize(2);
        assertThat(coordinator.isRegistered("G1")).isFalse();
    }

    // -----------------------------------------------------------------------
    // Timeout: incomplete batch → FAIL → DLQ
    // -----------------------------------------------------------------------

    @Test
    void timeout_incompleteBatch_routedToDlq_andRemovedFromCoordinator() throws InterruptedException {
        // Set timeout to 0 minutes so deadline = now (immediately expired)
        config.getBatch().setTimeoutMinutes(0);

        coordinator.register(tradeDoc("G1", 3, List.of("T1", "T2", "T3")));
        coordinator.recordProcessed(event("G1", "T1", "k1")); // only 1 of 3 arrives

        Thread.sleep(10); // ensure deadline has passed
        coordinator.checkTimeouts();

        verify(dlqPublisher).publishBatchFail(eq("G1"), eq(3), eq(1), any());
        assertThat(coordinator.isRegistered("G1")).isFalse();
    }

    @Test
    void timeout_completedBatch_notRoutedToDlq() throws InterruptedException {
        config.getBatch().setTimeoutMinutes(0);

        coordinator.register(tradeDoc("G1", 2, List.of("T1", "T2")));
        coordinator.recordProcessed(event("G1", "T1", "k1"));
        coordinator.recordProcessed(event("G1", "T2", "k2")); // batch completes → SUCCESS

        Thread.sleep(10);
        coordinator.checkTimeouts(); // nothing to timeout — batch already removed

        verify(dlqPublisher, never()).publishBatchFail(any(), any(int.class), any(int.class), any());
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private TradeDocMessage tradeDoc(String groupId, int expectedCount, List<String> tradeIds) {
        TradeDocMessage doc = new TradeDocMessage();
        doc.setTradeGroupId(groupId);
        doc.setExpectedCount(expectedCount);
        doc.setTradeIds(tradeIds);
        doc.setSource("cloud");
        doc.setPipelineVersion("1.0.0");
        doc.setSchemaId("banking_transaction_v1");
        return doc;
    }

    private ProcessedEvent event(String groupId, String tradeId, String idempotencyKey) {
        return ProcessedEvent.builder()
                .eventId("evt-" + idempotencyKey)
                .tradeGroupId(groupId)
                .tradeId(tradeId)
                .idempotencyKey(idempotencyKey)
                .eventType("banking_transaction")
                .timestamp("2024-01-01T00:00:00Z")
                .schemaId("banking_transaction_v1")
                .source("cloud")
                .pipelineVersion("1.0.0")
                .payload(Map.of("amount", 100.0))
                .build();
    }
}
