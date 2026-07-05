package com.adaptivedata.ingestion.intelligence.storage;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "ingestion.intelligence.parquet")
public class ParquetProperties {
    /** Flush a partition's buffer to a Parquet file once it holds this many events. */
    private int maxEventsPerFile = 10000;

    /** Flush every buffered partition on this interval, regardless of size. */
    private long flushIntervalMs = 60000;
}
