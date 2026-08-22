package com.adaptivedata.ingestion.metrics;

import com.adaptivedata.ingestion.intelligence.storage.ParquetStorageWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Run-level counters covering both the control-doc-readiness redesign and the
 * staging → Parquet writer-pool redesign, logged periodically so a single run's
 * behavior can be read straight off the log.
 *
 * "Control doc" counters are split from "other" causes (parse errors, exhausted
 * generic retries) so retry_count/total_events for the control-doc race
 * specifically can be measured. Per-file write stats are kept individually
 * (not just summed) so mean/median/per-writer breakdowns can be computed —
 * useful for the target-file-size and writer-thread-count experiments this
 * exists to support.
 */
@Component
public class IngestionMetrics {

    private static final Logger logger = LoggerFactory.getLogger(IngestionMetrics.class);

    private final Instant startTime = Instant.now();

    private final AtomicLong totalReceived = new AtomicLong();
    private final AtomicLong processedNormally = new AtomicLong();
    private final AtomicLong retryRoutedControlDoc = new AtomicLong();
    private final AtomicLong retryRoutedOther = new AtomicLong();
    private final AtomicLong retryRecovered = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong quarantinedControlDoc = new AtomicLong();
    private final AtomicLong quarantinedOther = new AtomicLong();

    private final AtomicInteger stagingQueueDepth = new AtomicInteger();
    private final AtomicInteger stagingQueuePeakDepth = new AtomicInteger();
    private volatile int writerThreadCount;

    private final List<FileWriteStat> fileStats = Collections.synchronizedList(new ArrayList<>());
    private final AtomicLong filesFailed = new AtomicLong();

    public void recordReceived(int count) {
        totalReceived.addAndGet(count);
    }

    public void recordProcessedNormally() {
        processedNormally.incrementAndGet();
    }

    /** Call only on an event's first pass (attempt_count == 0) routed to retry — counts distinct events, not retry attempts. */
    public void recordRetryRoutedControlDoc() {
        retryRoutedControlDoc.incrementAndGet();
    }

    public void recordRetryRoutedOther() {
        retryRoutedOther.incrementAndGet();
    }

    public void recordRetryRecovered() {
        retryRecovered.incrementAndGet();
    }

    public void recordDuplicate() {
        duplicates.incrementAndGet();
    }

    public void recordQuarantined(String reason) {
        if ("NO_CONTROL_DOC".equals(reason)) {
            quarantinedControlDoc.incrementAndGet();
        } else {
            quarantinedOther.incrementAndGet();
        }
    }

    public void setWriterThreadCount(int count) {
        this.writerThreadCount = count;
    }

    /** Called by StagingBuffer after every enqueue/dequeue so depth is always current. */
    public void recordStagingQueueDepth(int currentDepth) {
        stagingQueueDepth.set(currentDepth);
        stagingQueuePeakDepth.updateAndGet(peak -> Math.max(peak, currentDepth));
    }

    public void recordParquetFileWritten(int recordCount, long estimatedBytes, ParquetStorageWriter.WriteResult result, String writerThread) {
        if (!result.isSuccess()) {
            filesFailed.incrementAndGet();
            return;
        }
        fileStats.add(new FileWriteStat(recordCount, estimatedBytes, result.actualBytes(), result.writeMs(), result.putMs(), writerThread));
    }

    @Scheduled(fixedDelayString = "${ingestion.metrics.log-interval-ms:15000}")
    public void logSummary() {
        long total = totalReceived.get();
        if (total == 0) {
            return;
        }
        long retryTotal = retryRoutedControlDoc.get() + retryRoutedOther.get();
        double elapsedSec = Math.max(1, Duration.between(startTime, Instant.now()).toMillis()) / 1000.0;
        double retryPct = 100.0 * retryTotal / total;
        double eventsPerSec = total / elapsedSec;

        logger.info(
                "Ingestion metrics | total={} processed_normally={} retry_routed_control_doc={} "
                        + "retry_routed_other={} retry_recovered={} duplicates={} quarantined_control_doc={} "
                        + "quarantined_other={} retry_pct={}% elapsed_sec={} events_per_sec={} "
                        + "staging_queue_depth={} staging_queue_peak_depth={}",
                total, processedNormally.get(), retryRoutedControlDoc.get(), retryRoutedOther.get(),
                retryRecovered.get(), duplicates.get(), quarantinedControlDoc.get(), quarantinedOther.get(),
                String.format("%.2f", retryPct), String.format("%.1f", elapsedSec), String.format("%.1f", eventsPerSec),
                stagingQueueDepth.get(), stagingQueuePeakDepth.get());

        logWriterSummary();
    }

    private void logWriterSummary() {
        List<FileWriteStat> snapshot;
        synchronized (fileStats) {
            if (fileStats.isEmpty()) {
                return;
            }
            snapshot = new ArrayList<>(fileStats);
        }

        long[] records = snapshot.stream().mapToLong(FileWriteStat::recordCount).toArray();
        long[] estBytes = snapshot.stream().mapToLong(FileWriteStat::estimatedBytes).toArray();
        long[] actBytes = snapshot.stream().mapToLong(FileWriteStat::actualBytes).toArray();
        long[] writeMs = snapshot.stream().mapToLong(FileWriteStat::writeMs).toArray();
        long[] putMs = snapshot.stream().mapToLong(FileWriteStat::putMs).toArray();

        logger.info(
                "Parquet writer summary | writer_threads={} files_written={} files_failed={} "
                        + "records_per_file_mean={} records_per_file_median={} "
                        + "estimated_bytes_mean={} actual_bytes_mean={} actual_bytes_median={} "
                        + "parquet_write_ms_mean={} parquet_write_ms_median={} s3_put_ms_mean={} s3_put_ms_median={}",
                writerThreadCount, snapshot.size(), filesFailed.get(),
                mean(records), median(records),
                mean(estBytes), mean(actBytes), median(actBytes),
                mean(writeMs), median(writeMs), mean(putMs), median(putMs));

        Map<String, long[]> byWriter = new TreeMap<>();
        for (FileWriteStat s : snapshot) {
            byWriter.merge(s.writerThread(), new long[]{1, s.recordCount()},
                    (a, b) -> new long[]{a[0] + b[0], a[1] + b[1]});
        }
        byWriter.forEach((thread, counts) ->
                logger.info("Parquet writer stat | thread={} files={} records={}", thread, counts[0], counts[1]));
    }

    private static String mean(long[] values) {
        if (values.length == 0) {
            return "n/a";
        }
        double m = java.util.Arrays.stream(values).average().orElse(0);
        return String.format("%.1f", m);
    }

    private static String median(long[] values) {
        if (values.length == 0) {
            return "n/a";
        }
        long[] sorted = values.clone();
        java.util.Arrays.sort(sorted);
        int mid = sorted.length / 2;
        double m = sorted.length % 2 == 0 ? (sorted[mid - 1] + sorted[mid]) / 2.0 : sorted[mid];
        return String.format("%.1f", m);
    }

    private record FileWriteStat(int recordCount, long estimatedBytes, long actualBytes, long writeMs, long putMs, String writerThread) {
    }
}
