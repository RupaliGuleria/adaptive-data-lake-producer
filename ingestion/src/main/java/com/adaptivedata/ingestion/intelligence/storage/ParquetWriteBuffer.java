package com.adaptivedata.ingestion.intelligence.storage;

import com.adaptivedata.ingestion.intelligence.model.ValidatedEvent;
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

/**
 * Buffers PASSED/QUARANTINED events per partition (prefix + hour + source) so a single
 * Parquet file holds many documents instead of one-file-per-event. A partition flushes when
 * it reaches {@code maxEventsPerFile}, or on a fixed schedule regardless of size — whichever
 * comes first — so low-traffic partitions don't sit unwritten indefinitely.
 */
@Component
public class ParquetWriteBuffer {

    private static final Logger logger = LoggerFactory.getLogger(ParquetWriteBuffer.class);

    private final Object lock = new Object();
    private final Map<String, List<ValidatedEvent>> buffers = new HashMap<>();

    private final ParquetStorageWriter storageWriter;
    private final ParquetProperties properties;

    public ParquetWriteBuffer(ParquetStorageWriter storageWriter, ParquetProperties properties) {
        this.storageWriter = storageWriter;
        this.properties = properties;
    }

    public void add(String prefix, ValidatedEvent event) {
        String partitionKey = buildPartitionKey(prefix, event);
        List<ValidatedEvent> ready = null;

        synchronized (lock) {
            List<ValidatedEvent> bucket = buffers.computeIfAbsent(partitionKey, k -> new ArrayList<>());
            bucket.add(event);
            if (bucket.size() >= properties.getMaxEventsPerFile()) {
                ready = bucket;
                buffers.remove(partitionKey);
            }
        }

        if (ready != null) {
            storageWriter.writeBatch(partitionKey, ready);
        }
    }

    @Scheduled(fixedDelayString = "${ingestion.intelligence.parquet.flush-interval-ms:60000}")
    public void flushAll() {
        Map<String, List<ValidatedEvent>> toFlush;
        synchronized (lock) {
            if (buffers.isEmpty()) {
                return;
            }
            toFlush = new HashMap<>(buffers);
            buffers.clear();
        }
        logger.debug("Scheduled Parquet flush | partitions={}", toFlush.size());
        toFlush.forEach(storageWriter::writeBatch);
    }

    @PreDestroy
    public void flushOnShutdown() {
        logger.info("Flushing pending Parquet partitions before shutdown");
        flushAll();
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
}
