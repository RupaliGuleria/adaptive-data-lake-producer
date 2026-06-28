package com.adaptivedata.ingestion.intelligence.quality;

import java.util.Map;

public interface QualityRule {

    String ruleId();

    RuleResult evaluate(Map<String, Object> payload);
}
