package com.adaptivedata.ingestion.intelligence.storage;

import com.adaptivedata.ingestion.intelligence.model.ValidatedEvent;
import com.adaptivedata.ingestion.metrics.IngestionMetrics;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Buffers PASSED/QUARANTINED events per partition (prefix + hour + source) so a single
 * Parquet file holds many documents instead of one-file-per-event. A partition flushes when
 * its estimated size reaches {@code targetFileSizeMb}, or when it's gone idle for
 * {@code idleFlushMs} — whichever comes first — so low-traffic partitions don't sit
 * unwritten indefinitely.
 *
 * The actual write (Avro serialize + Snappy compress + MinIO PutObject) runs on a small
 * dedicated pool ({@code writerThreads}), not inline on the caller's thread. Profiling
 * showed compression alone can take multiple seconds for a large file; running that on
 * Spring's shared single-threaded @Scheduled executor stalled every other periodic task
 * for the duration of every flush.
 *
 * Deliberately NOT hash-routed to fixed writer "lanes" by partition key — the pool instead
 * pulls whichever partition's buffer is ready next. The partition key (year/month/day/hour/
 * source) is already the correct — and only — grouping boundary a Parquet file needs to
 * respect (see docs/PIPELINE_ARCHITECTURE.md: files already mix schema_id/trade_group_id
 * freely within a partition, by design). Fixed hash-routing by that same key would starve
 * most of the pool whenever a run's traffic lands in only one or two partitions, which is
 * exactly what this pipeline's own test bursts do — it would show no benefit from raising
 * writer-threads on the traffic patterns this system actually produces.
 */
@Component
public class ParquetWriteBuffer {

    private static final Logger logger = LoggerFactory.getLogger(ParquetWriteBuffer.class);

    /** Used for partitions with no prior flush yet to derive a real bytes-per-record figure from. */
    private static final long DEFAULT_BYTES_PER_RECORD_ESTIMATE = 200L;

    private final Object lock = new Object();
    private final Map<String, PartitionBuffer> buffers = new HashMap<>();

    /** Running actual-bytes-per-record from each partition's last real write, for better estimates on the next buffer. */
    private final Map<String, Double> lastKnownBytesPerRecord = new ConcurrentHashMap<>();

    private final ParquetStorageWriter storageWriter;
    private final ParquetProperties properties;
    private final IngestionMetrics metrics;
    private final ExecutorService writerPool;

    public ParquetWriteBuffer(ParquetStorageWriter storageWriter, ParquetProperties properties, IngestionMetrics metrics) {
        this.storageWriter = storageWriter;
        this.properties = properties;
        this.metrics = metrics;
        AtomicInteger threadIndex = new AtomicInteger();
        this.writerPool = Executors.newFixedThreadPool(properties.getWriterThreads(), r -> {
            Thread t = new Thread(r, "parquet-writer-" + threadIndex.getAndIncrement());
            t.setDaemon(true);
            return t;
        });
        metrics.setWriterThreadCount(properties.getWriterThreads());
    }

    public void add(String prefix, ValidatedEvent event) {
        String partitionKey = buildPartitionKey(prefix, event);
        List<ValidatedEvent> ready = null;

        synchronized (lock) {
            PartitionBuffer buf = buffers.computeIfAbsent(partitionKey, k -> new PartitionBuffer());
            buf.events.add(event);
            buf.estimatedBytes += lastKnownBytesPerRecord.getOrDefault(partitionKey, (double) DEFAULT_BYTES_PER_RECORD_ESTIMATE);
            buf.lastAddTime = System.currentTimeMillis();

            long targetBytes = properties.getTargetFileSizeMb() * 1024L * 1024L;
            if (buf.estimatedBytes >= targetBytes || buf.events.size() >= properties.getMaxEventsPerFileSafetyCap()) {
                ready = buf.events;
                buffers.remove(partitionKey);
            }
        }

        if (ready != null) {
            submitWrite(partitionKey, ready);
        }
    }

    /**
     * Flushes any partition buffer that's gone idle for idleFlushMs. This is the one
     * remaining @Scheduled task in the write path, and it's deliberately not the normal
     * data-movement mechanism — it only catches residual partial buffers that would
     * otherwise sit in memory indefinitely between bursts.
     */
    @Scheduled(fixedDelayString = "${ingestion.intelligence.parquet.idle-flush-ms:5000}")
    public void flushIdle() {
        long now = System.currentTimeMillis();
        List<Map.Entry<String, List<ValidatedEvent>>> toFlush = new ArrayList<>();

        synchronized (lock) {
            buffers.entrySet().removeIf(entry -> {
                PartitionBuffer buf = entry.getValue();
                if (!buf.events.isEmpty() && now - buf.lastAddTime >= properties.getIdleFlushMs()) {
                    toFlush.add(Map.entry(entry.getKey(), buf.events));
                    return true;
                }
                return false;
            });
        }

        for (Map.Entry<String, List<ValidatedEvent>> entry : toFlush) {
            logger.debug("Idle flush | partition={} events={}", entry.getKey(), entry.getValue().size());
            submitWrite(entry.getKey(), entry.getValue());
        }
    }

    private void submitWrite(String partitionKey, List<ValidatedEvent> events) {
        long estimatedBytes = Math.round(events.size()
                * lastKnownBytesPerRecord.getOrDefault(partitionKey, (double) DEFAULT_BYTES_PER_RECORD_ESTIMATE));
        writerPool.submit(() -> {
            ParquetStorageWriter.WriteResult result = storageWriter.writeBatch(partitionKey, events);
            if (result.isSuccess()) {
                lastKnownBytesPerRecord.put(partitionKey, (double) result.actualBytes() / events.size());
            }
            metrics.recordParquetFileWritten(events.size(), estimatedBytes, result, Thread.currentThread().getName());
        });
    }

    @PreDestroy
    public void flushOnShutdown() {
        logger.info("Flushing pending Parquet partitions before shutdown");
        Map<String, List<ValidatedEvent>> toFlush;
        synchronized (lock) {
            toFlush = new HashMap<>();
            buffers.forEach((k, v) -> toFlush.put(k, v.events));
            buffers.clear();
        }
        toFlush.forEach(this::submitWrite);

        writerPool.shutdown();
        try {
            // Give in-flight and just-submitted writes a real chance to land in MinIO
            // before the JVM exits — this is the actual data-safety guarantee, not the
            // submit() call above.
            if (!writerPool.awaitTermination(30, TimeUnit.SECONDS)) {
                logger.warn("Parquet writer pool did not drain within 30s of shutdown — some buffered events may not have been written");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private String buildPartitionKey(String prefix, ValidatedEvent event) {
        ZonedDateTime zdt = Instant.parse(event.getIngestionTime()).atZone(ZoneOffset.UTC);
        return String.format("%s/year=%d/month=%02d/day=%02d/hour=%02d/source=%s",
                prefix.replaceAll("^/|/$", ""),
                zdt.getYear(),
                zdt.getMonthValue(),
                zdt.getDayOfMonth(),
                zdt.getHour(),
                event.getEvent().getSource());
    }

    private static final class PartitionBuffer {
        final List<ValidatedEvent> events = new ArrayList<>();
        long estimatedBytes = 0;
        volatile long lastAddTime = System.currentTimeMillis();
    }
}
