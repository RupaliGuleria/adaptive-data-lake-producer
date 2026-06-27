package com.adaptivedata.ingestion.intelligence.quality;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class RuleResult {
    String ruleId;
    String field;
    Severity severity;
    boolean passed;
    String actualValue;
    String message;
}
