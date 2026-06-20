package com.adaptivedata.ingestion.consumer;

import com.adaptivedata.ingestion.model.BatchProcessorResult;
import com.adaptivedata.ingestion.processor.BatchProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class EventBatchListener {

    private static final Logger logger = LoggerFactory.getLogger(EventBatchListener.class);

    private final BatchProcessor batchProcessor;

    public EventBatchListener(BatchProcessor batchProcessor) {
        this.batchProcessor = batchProcessor;
    }

    @KafkaListener(
            topics = "${ingestion.topics.primary:banking-transactions}",
            groupId = "${spring.kafka.consumer.group-id:onprem-ingestion-group}",
            containerFactory = "batchListenerContainerFactory"
    )
    public void onEvents(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        try {
            BatchProcessorResult result = batchProcessor.process(records);
            logger.info("Poll done | total={} success={} duplicate={} retry={} dlq={}",
                    result.getTotal(), result.getSuccess(), result.getDuplicates(),
                    result.getRetryRouted(), result.getDlqRouted());
        } finally {
            // Always commit — errors are routed, not thrown, so the offset must advance
            ack.acknowledge();
        }
    }
}
