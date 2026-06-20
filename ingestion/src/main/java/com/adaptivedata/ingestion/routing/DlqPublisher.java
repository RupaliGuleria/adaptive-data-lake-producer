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
import java.util.Set;

@Component
public class DlqPublisher {

    private static final Logger logger = LoggerFactory.getLogger(DlqPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final IngestionConfig config;

    public DlqPublisher(KafkaTemplate<String, String> kafkaTemplate, IngestionConfig config) {
        this.kafkaTemplate = kafkaTemplate;
        this.config = config;
    }

    public void publish(ConsumerRecord<String, String> record, String failureReason, int attemptCount) {
        ProducerRecord<String, String> dlqRecord = new ProducerRecord<>(
                config.getTopics().getDlq(),
                record.key(),
                record.value()
        );
        dlqRecord.headers().add(new RecordHeader("failure_reason",
                failureReason.getBytes(StandardCharsets.UTF_8)));
        dlqRecord.headers().add(new RecordHeader("original_topic",
                record.topic().getBytes(StandardCharsets.UTF_8)));
        dlqRecord.headers().add(new RecordHeader("attempt_count",
                String.valueOf(attemptCount).getBytes(StandardCharsets.UTF_8)));

        kafkaTemplate.send(dlqRecord);
        logger.warn("DLQ | key={} reason={} attempt={}", record.key(), failureReason, attemptCount);
    }

    public void publishBatchFail(String tradeGroupId, int expectedCount, int actualCount, Set<String> missingTradeIds) {
        String payload = String.format(
                "{\"trade_group_id\":\"%s\",\"failure_reason\":\"BATCH_TIMEOUT\","
                        + "\"expected_count\":%d,\"actual_count\":%d,\"missing_count\":%d}",
                tradeGroupId, expectedCount, actualCount, missingTradeIds.size()
        );
        ProducerRecord<String, String> dlqRecord = new ProducerRecord<>(
                config.getTopics().getDlq(),
                tradeGroupId,
                payload
        );
        dlqRecord.headers().add(new RecordHeader("failure_reason",
                "BATCH_TIMEOUT".getBytes(StandardCharsets.UTF_8)));

        kafkaTemplate.send(dlqRecord);
        logger.error("Batch FAIL → DLQ | trade_group_id={} expected={} actual={} missing={}",
                tradeGroupId, expectedCount, actualCount, missingTradeIds.size());
    }
}
