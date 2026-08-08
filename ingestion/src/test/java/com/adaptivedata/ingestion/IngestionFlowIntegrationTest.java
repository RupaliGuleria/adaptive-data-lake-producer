package com.adaptivedata.ingestion;

import com.adaptivedata.ingestion.coordinator.BatchCoordinator;
import com.adaptivedata.ingestion.query.DuckDbQueryEngine;
import com.adaptivedata.ingestion.staging.StagingBuffer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@DirtiesContext
@EmbeddedKafka(
        partitions = 1,
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        topics = {
                "banking-control",
                "banking-transactions",
                "banking-transactions-retry",
                "banking-transactions-dlq"
        }
)
@TestPropertySource(properties = {
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "ingestion.staging.drain-interval-ms=999999999"   // disable drain during tests
})
class IngestionFlowIntegrationTest {

    // Prevent MinIO connection attempt (ensureBucket ApplicationRunner) and
    // DuckDB httpfs init (@PostConstruct) from killing the test context.
    @MockBean private S3Client s3Client;
    @MockBean private DuckDbQueryEngine duckDbQueryEngine;

    @Autowired private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired private StagingBuffer stagingBuffer;
    @Autowired private BatchCoordinator batchCoordinator;
    @Autowired private ObjectMapper objectMapper;

    // -----------------------------------------------------------------------
    // Happy path: TradeDoc + all matching events → batch SUCCESS → staged
    // -----------------------------------------------------------------------
    @Test
    void happyPath_allEventsReceived_batchSuccessAndStaged() {
        String gid = UUID.randomUUID().toString();
        int baseline = stagingBuffer.getTotalAdded();

        sendControlDoc(gid, 3, List.of("T1", "T2", "T3"));
        await().atMost(10, SECONDS).until(() -> batchCoordinator.isRegistered(gid));

        sendEvent(gid, "T1", key(gid, "T1"));
        sendEvent(gid, "T2", key(gid, "T2"));
        sendEvent(gid, "T3", key(gid, "T3"));

        await().atMost(20, SECONDS)
                .until(() -> stagingBuffer.getTotalAdded() - baseline >= 3);

        assertThat(stagingBuffer.getTotalAdded() - baseline).isEqualTo(3);
        // Batch removed from coordinator after SUCCESS
        assertThat(batchCoordinator.isRegistered(gid)).isFalse();
    }

    // -----------------------------------------------------------------------
    // Deduplication: duplicates filtered before count check.
    // expectedCount=5 with 7 messages (5 unique + 2 duplicates).
    // Proves dedup fires before the count check: staging must be exactly 5,
    // not 7, even though 7 messages were published.
    // -----------------------------------------------------------------------
    @Test
    void deduplication_duplicateEventsFiltered_onlyUniqueEventsReachStaging() throws InterruptedException {
        String gid = UUID.randomUUID().toString();
        int baseline = stagingBuffer.getTotalAdded();
        List<String> tradeIds = List.of("T1", "T2", "T3", "T4", "T5");

        sendControlDoc(gid, 5, tradeIds);
        await().atMost(10, SECONDS).until(() -> batchCoordinator.isRegistered(gid));

        // 5 unique events
        for (String tid : tradeIds) {
            sendEvent(gid, tid, key(gid, tid));
        }
        // 2 duplicates with the same idempotency key as T1 and T2
        sendEvent(gid, "T1", key(gid, "T1"));
        sendEvent(gid, "T2", key(gid, "T2"));

        // Batch SUCCESS fires when all 5 unique trade IDs are received.
        // Duplicates are absorbed by BatchState.addEvent (Set.add returns false)
        // so actualCount stays at 5, never 6 or 7.
        await().atMost(30, SECONDS)
                .until(() -> stagingBuffer.getTotalAdded() - baseline >= 5);

        // Allow any in-flight duplicate events to finish processing
        Thread.sleep(1_000);
        assertThat(stagingBuffer.getTotalAdded() - baseline).isEqualTo(5);
    }

    // -----------------------------------------------------------------------
    // No control doc: event without a registered TradeDoc → routed to DLQ
    // Staging buffer must NOT receive the event
    // -----------------------------------------------------------------------
    @Test
    void noControlDoc_eventWithoutTradeDoc_notStaged() throws InterruptedException {
        String gid = UUID.randomUUID().toString();
        int baseline = stagingBuffer.getTotalAdded();

        // Send event with no prior TradeDoc for this group
        sendEvent(gid, "T1", key(gid, "T1"));

        // Give the consumer time to process
        Thread.sleep(3_000);

        assertThat(stagingBuffer.getTotalAdded() - baseline).isEqualTo(0);
        assertThat(batchCoordinator.isRegistered(gid)).isFalse();
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private void sendControlDoc(String tradeGroupId, int expectedCount, List<String> tradeIds) {
        try {
            Map<String, Object> doc = Map.of(
                    "trade_group_id", tradeGroupId,
                    "expected_count", expectedCount,
                    "trade_ids", tradeIds,
                    "timestamp", "2024-01-01T00:00:00+00:00",
                    "source", "cloud",
                    "pipeline_version", "1.0.0",
                    "schema_id", "banking_transaction_v1"
            );
            kafkaTemplate.send("banking-control", tradeGroupId, objectMapper.writeValueAsString(doc));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private void sendEvent(String tradeGroupId, String tradeId, String idempotencyKey) {
        try {
            // Map.of() is limited to 10 entries; use ofEntries for 12-field envelope
            Map<String, Object> envelope = Map.ofEntries(
                    Map.entry("event_id", UUID.randomUUID().toString()),
                    Map.entry("event_version", "1.0"),
                    Map.entry("event_type", "banking_transaction"),
                    Map.entry("trace_id", UUID.randomUUID().toString()),
                    Map.entry("idempotency_key", idempotencyKey),
                    Map.entry("timestamp", "2024-01-01T00:00:00+00:00"),
                    Map.entry("source", "cloud"),
                    Map.entry("pipeline_version", "1.0.0"),
                    Map.entry("schema_id", "banking_transaction_v1"),
                    Map.entry("trade_group_id", tradeGroupId),
                    Map.entry("trade_id", tradeId),
                    Map.entry("payload", Map.of("amount", 100.0))
            );
            kafkaTemplate.send("banking-transactions", idempotencyKey, objectMapper.writeValueAsString(envelope));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /** Stable, unique idempotency key per trade group + trade id combination. */
    private String key(String tradeGroupId, String tradeId) {
        return tradeGroupId + "-" + tradeId;
    }
}
