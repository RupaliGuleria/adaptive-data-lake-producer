package com.adaptivedata.ingestion.consumer;

import com.adaptivedata.ingestion.model.BatchProcessorResult;
import com.adaptivedata.ingestion.processor.BatchProcessor;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

@Component
public class RetryIngestionListener {

    private static final Logger logger = LoggerFactory.getLogger(RetryIngestionListener.class);

    private final BatchProcessor batchProcessor;

    // Not-yet-due retries wait here instead of on a blocking Thread.sleep() in the
    // Kafka consumer thread. A single dedicated thread (below) drains this queue,
    // batching together everything that comes due in the same moment so retries
    // still go through BatchProcessor's real 8-worker pool as one call — a first
    // attempt at this fix used one ScheduledExecutorService.schedule() per record,
    // which stayed non-blocking but serialized retries one at a time and measured
    // no faster than the original blocking design; this batches them instead.
    private final DelayQueue<PendingRetry> pending = new DelayQueue<>();
    private final Thread drainThread;
    private volatile boolean running = true;

    public RetryIngestionListener(BatchProcessor batchProcessor) {
        this.batchProcessor = batchProcessor;
        this.drainThread = new Thread(this::drainLoop, "retry-drain");
        this.drainThread.setDaemon(true);
        this.drainThread.start();
    }

    @KafkaListener(
            topics = "${ingestion.topics.retry:banking-transactions-retry}",
            groupId = "${spring.kafka.consumer.group-id:onprem-ingestion-group}-retry",
            containerFactory = "batchListenerContainerFactory"
    )
    public void onRetryEvents(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        for (ConsumerRecord<String, String> record : records) {
            pending.put(new PendingRetry(record, getRetryAfter(record)));
        }
        // Acking as soon as a record is durably queued in-memory — not after it's
        // actually processed by the drain loop below — is what keeps this consumer
        // thread free to keep polling instead of stalling behind the longest
        // backoff in the batch. See the durability note on PendingRetry.
        ack.acknowledge();
    }

    /**
     * Runs for the service's lifetime on one dedicated thread. take() parks
     * (no busy-wait) until the earliest-due record's delay elapses, then
     * drainTo() sweeps up everything else already due at that moment so they
     * process together as one batch.
     */
    private void drainLoop() {
        while (running) {
            try {
                PendingRetry first = pending.take();
                List<PendingRetry> due = new ArrayList<>();
                due.add(first);
                pending.drainTo(due);

                List<ConsumerRecord<String, String>> records = new ArrayList<>(due.size());
                for (PendingRetry p : due) {
                    records.add(p.record());
                }

                BatchProcessorResult result = batchProcessor.process(records);
                logger.info("Retry drain | total={} success={} dlq={}",
                        result.getTotal(), result.getSuccess(), result.getDlqRouted());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.error("Retry drain loop error", e);
            }
        }
    }

    private long getRetryAfter(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader("retry_after");
        if (header == null) {
            return System.currentTimeMillis();
        }
        try {
            return Long.parseLong(new String(header.value(), StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            return System.currentTimeMillis();
        }
    }

    @PreDestroy
    public void shutdown() {
        running = false;
        drainThread.interrupt();
    }

    /**
     * Durability tradeoff: offsets commit as soon as a retry record is queued
     * in-memory, not once it's actually processed. A JVM crash in that narrow
     * window (record queued, backoff not yet elapsed) loses it from the durable
     * log rather than redelivering it on restart. For this pipeline that window
     * is at most a few seconds and the record was already a retry (not fresh
     * data), so the cost is bounded; closing it fully would need a durable delay
     * mechanism (e.g. re-publishing to Kafka with a delay/timer wheel), which is
     * more infrastructure than this fix's scope.
     */
    private record PendingRetry(ConsumerRecord<String, String> record, long readyAtMillis) implements Delayed {
        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(readyAtMillis - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            return Long.compare(readyAtMillis, ((PendingRetry) o).readyAtMillis);
        }
    }
}
