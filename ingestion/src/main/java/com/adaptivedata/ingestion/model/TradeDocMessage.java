package com.adaptivedata.ingestion.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class TradeDocMessage {

    @JsonProperty("trade_group_id")
    private String tradeGroupId;

    @JsonProperty("expected_count")
    private int expectedCount;

    @JsonProperty("trade_ids")
    private List<String> tradeIds;

    private String timestamp;
    private String source;

    @JsonProperty("pipeline_version")
    private String pipelineVersion;

    @JsonProperty("schema_id")
    private String schemaId;
}
