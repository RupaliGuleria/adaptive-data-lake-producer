package com.adaptivedata.ingestion.processor;

import com.adaptivedata.ingestion.config.IngestionConfig;
import com.adaptivedata.ingestion.coordinator.BatchCoordinator;
import com.adaptivedata.ingestion.model.BatchProcessorResult;
import com.adaptivedata.ingestion.model.EventEnvelope;
import com.adaptivedata.ingestion.model.EventResult;
import com.adaptivedata.ingestion.model.ProcessedEvent;
import com.adaptivedata.ingestion.routing.DlqPublisher;
import com.adaptivedata.ingestion.routing.RetryPublisher;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Component
public class BatchProcessor {

    private static final Logger logger = LoggerFactory.getLogger(BatchProcessor.class);

    private final ExecutorService workerPool;
    private final DeduplicationStore deduplicationStore;
    private final ErrorClassifier errorClassifier;
    private final BatchCoordinator batchCoordinator;
    private final RetryPublisher retryPublisher;
    private final DlqPublisher dlqPublisher;
    private final ObjectMapper objectMapper;
    private final IngestionConfig config;

    public BatchProcessor(
            DeduplicationStore deduplicationStore,
            ErrorClassifier errorClassifier,
            BatchCoordinator batchCoordinator,
            RetryPublisher retryPublisher,
            DlqPublisher dlqPublisher,
            ObjectMapper objectMapper,
            IngestionConfig config) {
        this.deduplicationStore = deduplicationStore;
        this.errorClassifier = errorClassifier;
        this.batchCoordinator = batchCoordinator;
        this.retryPublisher = retryPublisher;
        this.dlqPublisher = dlqPublisher;
        this.objectMapper = objectMapper;
        this.config = config;
        this.workerPool = Executors.newFixedThreadPool(config.getWorkerPoolSize());
    }

    public BatchProcessorResult process(List<ConsumerRecord<String, String>> records) {
        if (records.isEmpty()) {
            return BatchProcessorResult.builder().build();
        }

        int numWorkers = Math.min(config.getWorkerPoolSize(), records.size());
        int sliceSize = (int) Math.ceil((double) records.size() / numWorkers);

        List<CompletableFuture<List<EventResult>>> futures = IntStream.range(0, numWorkers)
                .mapToObj(i -> {
                    int from = i * sliceSize;
                    int to = Math.min(from + sliceSize, records.size());
                    List<ConsumerRecord<String, String>> slice = records.subList(from, to);
                    return CompletableFuture.supplyAsync(() -> processSlice(slice), workerPool);
                })
                .collect(Collectors.toList());

        List<EventResult> allResults = futures.stream()
                .flatMap(f -> f.join().stream())
                .collect(Collectors.toList());

        return BatchProcessorResult.from(allResults);
    }

    private List<EventResult> processSlice(List<ConsumerRecord<String, String>> slice) {
        List<EventResult> results = new ArrayList<>(slice.size());
        for (ConsumerRecord<String, String> record : slice) {
            results.add(processOne(record));
        }
        return results;
    }

    private EventResult processOne(ConsumerRecord<String, String> record) {
        // 1. Parse envelope
        EventEnvelope envelope;
        try {
            envelope = objectMapper.readValue(record.value(), EventEnvelope.class);
        } catch (Exception e) {
            dlqPublisher.publish(record, "FATAL_PARSE_ERROR", getAttemptCount(record));
            return EventResult.DLQ_ROUTED;
        }

        // 2. Dedup check
        if (deduplicationStore.isDuplicate(envelope.getIdempotencyKey())) {
            return EventResult.DUPLICATE;
        }

        // 3. Validate required envelope fields
        if (envelope.getTradeGroupId() == null || envelope.getTradeId() == null) {
            dlqPublisher.publish(record, "FATAL_MISSING_FIELDS", getAttemptCount(record));
            return EventResult.DLQ_ROUTED;
        }

        // 4. Ensure control doc is registered for this trade group
        if (!batchCoordinator.isRegistered(envelope.getTradeGroupId())) {
            dlqPublisher.publish(record, "NO_CONTROL_DOC", getAttemptCount(record));
            return EventResult.DLQ_ROUTED;
        }

        // 5. Build and record the processed event
        try {
            ProcessedEvent processed = ProcessedEvent.builder()
                    .eventId(envelope.getEventId())
                    .tradeGroupId(envelope.getTradeGroupId())
                    .tradeId(envelope.getTradeId())
                    .idempotencyKey(envelope.getIdempotencyKey())
                    .eventType(envelope.getEventType())
                    .timestamp(envelope.getTimestamp())
                    .schemaId(envelope.getSchemaId())
                    .source(envelope.getSource())
                    .pipelineVersion(envelope.getPipelineVersion())
                    .payload(envelope.getPayload())
                    .build();

            deduplicationStore.mark(envelope.getIdempotencyKey());
            batchCoordinator.recordProcessed(processed);
            return EventResult.SUCCESS;
        } catch (Exception e) {
            return routeError(record, e);
        }
    }

    private EventResult routeError(ConsumerRecord<String, String> record, Exception e) {
        int attemptCount = getAttemptCount(record);
        ErrorClassifier.ErrorType errorType = errorClassifier.classify(e);

        if (errorType == ErrorClassifier.ErrorType.FATAL) {
            dlqPublisher.publish(record, "FATAL:" + e.getClass().getSimpleName(), attemptCount);
            return EventResult.DLQ_ROUTED;
        }

        if (attemptCount >= config.getRetry().getMaxAttempts()) {
            dlqPublisher.publish(record, "RETRY_EXHAUSTED", attemptCount);
            return EventResult.DLQ_ROUTED;
        }

        retryPublisher.publish(record, attemptCount + 1);
        return EventResult.RETRY_ROUTED;
    }

    private int getAttemptCount(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader("attempt_count");
        if (header == null) return 0;
        try {
            return Integer.parseInt(new String(header.value(), StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
