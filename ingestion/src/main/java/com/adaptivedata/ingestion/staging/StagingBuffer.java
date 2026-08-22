package com.adaptivedata.ingestion.staging;

import com.adaptivedata.ingestion.config.IngestionConfig;
import com.adaptivedata.ingestion.intelligence.IntelligenceProcessor;
import com.adaptivedata.ingestion.metrics.IngestionMetrics;
import com.adaptivedata.ingestion.model.ProcessedEvent;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hands processed events from Kafka-consumer worker threads off to the intelligence/
 * Parquet-write pipeline via a bounded queue, drained continuously by one dedicated
 * thread — not on a fixed schedule.
 *
 * Previously this drained at most 1,000 events every 5 seconds
 * ({@code @Scheduled(fixedDelay = 5000)} + {@code drainTo(batch, 1000)}), an artificial
 * 200 events/sec ceiling independent of how fast every other stage could actually run.
 * The consumer thread below instead blocks efficiently on {@link LinkedBlockingQueue#poll}
 * and reacts as soon as anything is available — no wait interval limits normal throughput.
 *
 * The queue is bounded ({@code ingestion.staging.queue-capacity}) and {@code add}/
 * {@code addAll} block (via {@code put}) rather than silently dropping when full, so a
 * writer stage that falls behind produces real backpressure — visible in
 * {@link IngestionMetrics}'s queue-depth counters — propagating back through
 * {@code BatchCoordinator} to the Kafka consumer's worker pool, instead of unbounded
 * memory growth.
 */
@Component
public class StagingBuffer {

    private static final Logger logger = LoggerFactory.getLogger(StagingBuffer.class);

    /** Cap on how much a single drain-and-process cycle grabs at once — a call-size limit
     *  for IntelligenceProcessor, not a throughput limiter: the loop re-polls immediately. */
    private static final int MAX_BATCH_SIZE = 5_000;
    private static final long POLL_TIMEOUT_MS = 200;

    private final LinkedBlockingQueue<ProcessedEvent> buffer;
    private final AtomicInteger totalAdded = new AtomicInteger(0);
    private final IntelligenceProcessor intelligenceProcessor;
    private final IngestionMetrics metrics;

    private final Thread consumerThread;
    private volatile boolean running = true;

    public StagingBuffer(IntelligenceProcessor intelligenceProcessor, IngestionConfig config, IngestionMetrics metrics) {
        this.intelligenceProcessor = intelligenceProcessor;
        this.metrics = metrics;
        this.buffer = new LinkedBlockingQueue<>(config.getStaging().getQueueCapacity());
        this.consumerThread = new Thread(this::consumeLoop, "staging-drain");
        this.consumerThread.setDaemon(true);
        this.consumerThread.start();
    }

    public void add(ProcessedEvent event) {
        try {
            buffer.put(event);
            totalAdded.incrementAndGet();
            metrics.recordStagingQueueDepth(buffer.size());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void addAll(Collection<ProcessedEvent> events) {
        try {
            for (ProcessedEvent event : events) {
                buffer.put(event);
            }
            totalAdded.addAndGet(events.size());
            metrics.recordStagingQueueDepth(buffer.size());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public int size() {
        return buffer.size();
    }

    /** Total events ever added — does not decrement on drain. Safe for test assertions. */
    public int getTotalAdded() {
        return totalAdded.get();
    }

    private void consumeLoop() {
        while (running) {
            try {
                ProcessedEvent first = buffer.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue; // nothing available — loop back and block again, no busy-wait
                }
                List<ProcessedEvent> batch = new ArrayList<>(MAX_BATCH_SIZE);
                batch.add(first);
                buffer.drainTo(batch, MAX_BATCH_SIZE - 1);
                metrics.recordStagingQueueDepth(buffer.size());

                long start = System.nanoTime();
                intelligenceProcessor.processBatch(batch);
                long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                logger.info("Drained {} events → intelligence layer | elapsed_ms={} queue_remaining={}",
                        batch.size(), elapsedMs, buffer.size());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.error("Staging drain loop error", e);
            }
        }
    }

    @PreDestroy
    public void shutdown() {
        logger.info("Stopping staging drain, flushing remaining queued events");
        running = false;
        consumerThread.interrupt();
        try {
            consumerThread.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // The loop above may have been mid-poll when interrupted, or may have exited
        // with events still queued behind whatever it was processing — drain and
        // process the rest synchronously here so nothing staged is lost on shutdown.
        List<ProcessedEvent> remaining = new ArrayList<>();
        buffer.drainTo(remaining);
        if (!remaining.isEmpty()) {
            logger.info("Processing {} events queued at shutdown", remaining.size());
            intelligenceProcessor.processBatch(remaining);
        }
    }
}
