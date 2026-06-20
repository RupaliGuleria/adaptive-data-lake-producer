package com.adaptivedata.ingestion.coordinator;

import com.adaptivedata.ingestion.config.IngestionConfig;
import com.adaptivedata.ingestion.model.ProcessedEvent;
import com.adaptivedata.ingestion.model.TradeDocMessage;
import com.adaptivedata.ingestion.routing.DlqPublisher;
import com.adaptivedata.ingestion.staging.StagingBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class BatchCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(BatchCoordinator.class);

    private final ConcurrentHashMap<String, BatchState> activeBatches = new ConcurrentHashMap<>();
    private final StagingBuffer stagingBuffer;
    private final DlqPublisher dlqPublisher;
    private final IngestionConfig config;

    public BatchCoordinator(StagingBuffer stagingBuffer, DlqPublisher dlqPublisher, IngestionConfig config) {
        this.stagingBuffer = stagingBuffer;
        this.dlqPublisher = dlqPublisher;
        this.config = config;
    }

    public void register(TradeDocMessage tradeDoc) {
        Instant deadline = Instant.now().plus(config.getBatch().getTimeoutMinutes(), ChronoUnit.MINUTES);
        BatchState state = new BatchState(
                tradeDoc.getTradeGroupId(),
                tradeDoc.getExpectedCount(),
                new HashSet<>(tradeDoc.getTradeIds()),
                deadline
        );
        BatchState existing = activeBatches.putIfAbsent(tradeDoc.getTradeGroupId(), state);
        if (existing != null) {
            logger.warn("Duplicate TradeDoc for active batch | trade_group_id={}", tradeDoc.getTradeGroupId());
        } else {
            logger.info("Batch registered | trade_group_id={} expected={} deadline={}",
                    tradeDoc.getTradeGroupId(), tradeDoc.getExpectedCount(), deadline);
        }
    }

    public boolean isRegistered(String tradeGroupId) {
        return activeBatches.containsKey(tradeGroupId);
    }

    public void recordProcessed(ProcessedEvent event) {
        BatchState state = activeBatches.get(event.getTradeGroupId());
        if (state == null) return;

        state.addEvent(event);

        if (state.tryComplete()) {
            stagingBuffer.addAll(state.drainEvents());
            activeBatches.remove(event.getTradeGroupId());
            logger.info("Batch SUCCESS | trade_group_id={} count={}",
                    event.getTradeGroupId(), state.getActualCount());
        }
    }

    @Scheduled(fixedDelayString = "${ingestion.batch.timeout-check-interval-ms:30000}")
    public void checkTimeouts() {
        Instant now = Instant.now();
        activeBatches.entrySet().removeIf(entry -> {
            BatchState state = entry.getValue();
            if (state.isTimedOut(now) && state.markFail()) {
                dlqPublisher.publishBatchFail(
                        state.getTradeGroupId(),
                        state.getExpectedCount(),
                        state.getActualCount(),
                        state.getMissingTradeIds()
                );
                return true;
            }
            return false;
        });
    }
}
