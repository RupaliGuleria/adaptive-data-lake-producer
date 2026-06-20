package com.adaptivedata.ingestion.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.Map;

@Data
public class EventEnvelope {

    @JsonProperty("event_id")
    private String eventId;

    @JsonProperty("event_version")
    private String eventVersion;

    @JsonProperty("event_type")
    private String eventType;

    @JsonProperty("trace_id")
    private String traceId;

    @JsonProperty("idempotency_key")
    private String idempotencyKey;

    private String timestamp;
    private String source;

    @JsonProperty("pipeline_version")
    private String pipelineVersion;

    @JsonProperty("schema_id")
    private String schemaId;

    @JsonProperty("trade_group_id")
    private String tradeGroupId;

    @JsonProperty("trade_id")
    private String tradeId;

    private Map<String, Object> payload;
}
