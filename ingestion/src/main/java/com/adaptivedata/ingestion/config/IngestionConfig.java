package com.adaptivedata.ingestion.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "ingestion")
@Data
public class IngestionConfig {

    private int workerPoolSize = 8;
    private int dedupCacheTtlMinutes = 60;
    private BatchProperties batch = new BatchProperties();
    private RetryProperties retry = new RetryProperties();
    private TopicsProperties topics = new TopicsProperties();
    private StagingProperties staging = new StagingProperties();

    @Data
    public static class BatchProperties {
        private int timeoutMinutes = 5;
        private long timeoutCheckIntervalMs = 30_000;
    }

    @Data
    public static class StagingProperties {
        /**
         * Bounded so a slow/stuck writer stage produces visible backpressure
         * (callers block on put()) instead of unbounded memory growth. Sized
         * generously above what a single burst realistically queues.
         */
        private int queueCapacity = 200_000;
    }

    @Data
    public static class RetryProperties {
        private int maxAttempts = 3;
        private long backoffBaseMs = 2_000;
    }

    @Data
    public static class TopicsProperties {
        private String control = "banking-control";
        private String primary = "banking-transactions";
        private String retry = "banking-transactions-retry";
        private String dlq = "banking-transactions-dlq";
    }
}
