package com.adaptivedata.ingestion.consumer;

import com.adaptivedata.ingestion.coordinator.BatchCoordinator;
import com.adaptivedata.ingestion.model.TradeDocMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ControlDocListener {

    private static final Logger logger = LoggerFactory.getLogger(ControlDocListener.class);

    private final BatchCoordinator batchCoordinator;
    private final ObjectMapper objectMapper;

    public ControlDocListener(BatchCoordinator batchCoordinator, ObjectMapper objectMapper) {
        this.batchCoordinator = batchCoordinator;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "${ingestion.topics.control:banking-control}",
            groupId = "${spring.kafka.consumer.group-id:onprem-ingestion-group}",
            containerFactory = "batchListenerContainerFactory"
    )
    public void onControlDocs(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        for (ConsumerRecord<String, String> record : records) {
            try {
                TradeDocMessage tradeDoc = objectMapper.readValue(record.value(), TradeDocMessage.class);
                batchCoordinator.register(tradeDoc);
            } catch (Exception e) {
                logger.error("Failed to parse TradeDoc | key={} error={}", record.key(), e.getMessage());
            }
        }
        ack.acknowledge();
    }
}
 