package com.adaptivedata.ingestion.intelligence.quality.rules;

import com.adaptivedata.ingestion.intelligence.quality.QualityRule;
import com.adaptivedata.ingestion.intelligence.quality.RuleResult;
import com.adaptivedata.ingestion.intelligence.quality.Severity;

import java.util.Map;

/**
 * Reusable null-check rule. Instantiated per field in QualityRulesConfig.
 * Caller sets the field name and severity so the same class covers
 * CRITICAL required fields and lower-severity optional presence checks.
 */
public class NotNullRule implements QualityRule {

    private final String field;
    private final Severity severity;
    private final String schemaId;

    public NotNullRule(String field, Severity severity, String schemaId) {
        this.field = field;
        this.severity = severity;
        this.schemaId = schemaId;
    }

    @Override
    public String ruleId() {
        return field + ".not_null";
    }

    @Override
    public boolean appliesTo(String schemaId) {
        return this.schemaId.equals(schemaId);
    }

    @Override
    public RuleResult evaluate(Map<String, Object> payload) {
        Object value = payload.get(field);
        boolean passed = value != null && !value.toString().isBlank();
        return RuleResult.builder()
                .ruleId(ruleId())
                .field(field)
                .severity(severity)
                .passed(passed)
                .actualValue(value != null ? value.toString() : "null")
                .message(passed ? null : field + " must not be null or blank")
                .build();
    }
}
