package com.adaptivedata.ingestion.model;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder
public class ProcessedEvent {
    private String eventId;
    private String tradeGroupId;
    private String tradeId;
    private String idempotencyKey;
    private String eventType;
    private String timestamp;
    private String schemaId;
    private String source;
    private String pipelineVersion;
    private Map<String, Object> payload;
}
