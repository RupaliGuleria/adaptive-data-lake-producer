package com.adaptivedata.ingestion.staging;

import com.adaptivedata.ingestion.intelligence.IntelligenceProcessor;
import com.adaptivedata.ingestion.model.ProcessedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class StagingBuffer {

    private static final Logger logger = LoggerFactory.getLogger(StagingBuffer.class);
    private static final int DRAIN_BATCH_SIZE = 1_000;

    private final LinkedBlockingQueue<ProcessedEvent> buffer = new LinkedBlockingQueue<>();
    private final AtomicInteger totalAdded = new AtomicInteger(0);
    private final IntelligenceProcessor intelligenceProcessor;

    public StagingBuffer(IntelligenceProcessor intelligenceProcessor) {
        this.intelligenceProcessor = intelligenceProcessor;
    }

    public void add(ProcessedEvent event) {
        buffer.offer(event);
        totalAdded.incrementAndGet();
    }

    public void addAll(Collection<ProcessedEvent> events) {
        buffer.addAll(events);
        totalAdded.addAndGet(events.size());
    }

    public int size() {
        return buffer.size();
    }

    /** Total events ever added — does not decrement on drain. Safe for test assertions. */
    public int getTotalAdded() {
        return totalAdded.get();
    }

    @Scheduled(fixedDelayString = "${ingestion.staging.drain-interval-ms:5000}")
    public void drain() {
        List<ProcessedEvent> batch = new ArrayList<>(DRAIN_BATCH_SIZE);
        buffer.drainTo(batch, DRAIN_BATCH_SIZE);
        if (!batch.isEmpty()) {
            logger.info("Draining {} events → intelligence layer", batch.size());
            intelligenceProcessor.processBatch(batch);
        }
    }
}
