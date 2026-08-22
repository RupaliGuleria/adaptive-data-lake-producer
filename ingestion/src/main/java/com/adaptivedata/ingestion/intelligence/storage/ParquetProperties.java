package com.adaptivedata.ingestion.intelligence.storage;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "ingestion.intelligence.parquet")
public class ParquetProperties {
    /** Primary flush trigger: target Parquet file size, in MB (estimated pre-write bytes). */
    private int targetFileSizeMb = 32;

    /**
     * Hard safety cap on events per file, independent of size — guards against
     * unbounded buffer growth if per-event size estimates run low (e.g. an
     * unexpectedly high volume of tiny records). Not the normal flush trigger.
     */
    private int maxEventsPerFileSafetyCap = 200_000;

    /**
     * A partition buffer with no new events for this long gets flushed regardless
     * of size, so low-traffic partitions don't sit unwritten indefinitely. Also
     * the interval on which the idle sweep itself runs.
     */
    private long idleFlushMs = 5000;

    /**
     * Writer threads dedicated to the actual Parquet write (Avro serialize + Snappy
     * compress) + MinIO PutObject. Profiling showed compression alone can take
     * several seconds for a large file — running that inline on whatever thread
     * decided to flush would block it; a dedicated pool lets multiple partitions'
     * flushes (or successive flushes of the same partition) proceed concurrently.
     * Configurable specifically to benchmark 1/2/4/8 writers against MinIO/disk
     * throughput.
     */
    private int writerThreads = 8;
}
