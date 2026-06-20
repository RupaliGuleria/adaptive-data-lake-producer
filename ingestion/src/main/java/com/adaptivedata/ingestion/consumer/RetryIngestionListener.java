package com.adaptivedata.ingestion.consumer;

import com.adaptivedata.ingestion.model.BatchProcessorResult;
import com.adaptivedata.ingestion.processor.BatchProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Component
public class RetryIngestionListener {

    private static final Logger logger = LoggerFactory.getLogger(RetryIngestionListener.class);

    private final BatchProcessor batchProcessor;

    public RetryIngestionListener(BatchProcessor batchProcessor) {
        this.batchProcessor = batchProcessor;
    }

    @KafkaListener(
            topics = "${ingestion.topics.retry:banking-transactions-retry}",
            groupId = "${spring.kafka.consumer.group-id:onprem-ingestion-group}-retry",
            containerFactory = "batchListenerContainerFactory"
    )
    public void onRetryEvents(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        enforceBackoff(records);
        try {
            BatchProcessorResult result = batchProcessor.process(records);
            logger.info("Retry poll done | total={} success={} dlq={}",
                    result.getTotal(), result.getSuccess(), result.getDlqRouted());
        } finally {
            ack.acknowledge();
        }
    }

    /**
     * Blocks until the latest retry_after timestamp in the batch has elapsed.
     * Max sleep is 8s (attempt 3 backoff) — safely within max.poll.interval.ms.
     */
    private void enforceBackoff(List<ConsumerRecord<String, String>> records) {
        long maxRetryAfter = records.stream()
                .mapToLong(this::getRetryAfter)
                .max()
                .orElse(0L);

        long waitMs = maxRetryAfter - System.currentTimeMillis();
        if (waitMs > 0) {
            try {
                Thread.sleep(waitMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private long getRetryAfter(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader("retry_after");
        if (header == null) return 0L;
        try {
            return Long.parseLong(new String(header.value(), StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
}
