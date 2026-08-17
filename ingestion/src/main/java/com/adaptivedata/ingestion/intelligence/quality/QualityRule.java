package com.adaptivedata.ingestion.intelligence.quality;

import java.util.Map;

public interface QualityRule {

    String ruleId();

    /** Which schema_id this rule evaluates against — rules for one domain must not score events from another. */
    boolean appliesTo(String schemaId);

    RuleResult evaluate(Map<String, Object> payload);
}
