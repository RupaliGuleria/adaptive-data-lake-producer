package com.adaptivedata.ingestion.routing;

import com.adaptivedata.ingestion.config.IngestionConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class RetryPublisher {

    private static final Logger logger = LoggerFactory.getLogger(RetryPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final IngestionConfig config;

    public RetryPublisher(KafkaTemplate<String, String> kafkaTemplate, IngestionConfig config) {
        this.kafkaTemplate = kafkaTemplate;
        this.config = config;
    }

    public void publish(ConsumerRecord<String, String> record, int nextAttemptCount) {
        long backoffMs = config.getRetry().getBackoffBaseMs() * (long) Math.pow(2, nextAttemptCount - 1);
        long retryAfter = System.currentTimeMillis() + backoffMs;

        ProducerRecord<String, String> retryRecord = new ProducerRecord<>(
                config.getTopics().getRetry(),
                record.key(),
                record.value()
        );
        retryRecord.headers().add(new RecordHeader("attempt_count",
                String.valueOf(nextAttemptCount).getBytes(StandardCharsets.UTF_8)));
        retryRecord.headers().add(new RecordHeader("retry_after",
                String.valueOf(retryAfter).getBytes(StandardCharsets.UTF_8)));

        kafkaTemplate.send(retryRecord);
        logger.info("Retry | key={} attempt={} backoff_ms={}", record.key(), nextAttemptCount, backoffMs);
    }
}
