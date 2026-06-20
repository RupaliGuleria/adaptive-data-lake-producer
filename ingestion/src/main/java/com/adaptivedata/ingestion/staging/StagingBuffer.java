package com.adaptivedata.ingestion.staging;

import com.adaptivedata.ingestion.model.ProcessedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

@Component
public class StagingBuffer {

    private static final Logger logger = LoggerFactory.getLogger(StagingBuffer.class);
    private static final int DRAIN_BATCH_SIZE = 1_000;

    private final LinkedBlockingQueue<ProcessedEvent> buffer = new LinkedBlockingQueue<>();

    public void add(ProcessedEvent event) {
        buffer.offer(event);
    }

    public void addAll(Collection<ProcessedEvent> events) {
        buffer.addAll(events);
    }

    public int size() {
        return buffer.size();
    }

    // Phase 2: replace this drain target with a MinIO S3 write — no other changes needed
    @Scheduled(fixedDelayString = "${ingestion.staging.drain-interval-ms:5000}")
    public void drain() {
        List<ProcessedEvent> batch = new ArrayList<>(DRAIN_BATCH_SIZE);
        buffer.drainTo(batch, DRAIN_BATCH_SIZE);
        if (!batch.isEmpty()) {
            logger.info("Drained {} events from staging buffer → intelligence layer", batch.size());
            // TODO: forward batch to intelligence layer
        }
    }
}
