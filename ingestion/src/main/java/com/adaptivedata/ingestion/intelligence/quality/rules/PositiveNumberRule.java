package com.adaptivedata.ingestion.intelligence.quality.rules;

import com.adaptivedata.ingestion.intelligence.quality.QualityRule;
import com.adaptivedata.ingestion.intelligence.quality.RuleResult;
import com.adaptivedata.ingestion.intelligence.quality.Severity;

import java.util.Map;

/**
 * Reusable positive-number rule. Instantiated per field in QualityRulesConfig,
 * same pattern as {@link NotNullRule} — covers "amount must be > 0" for banking
 * and its TPC-H equivalents (o_totalprice, l_quantity) without duplicating logic.
 */
public class PositiveNumberRule implements QualityRule {

    private final String field;
    private final Severity severity;
    private final String schemaId;

    public PositiveNumberRule(String field, Severity severity, String schemaId) {
        this.field = field;
        this.severity = severity;
        this.schemaId = schemaId;
    }

    @Override
    public String ruleId() {
        return field + ".positive";
    }

    @Override
    public boolean appliesTo(String schemaId) {
        return this.schemaId.equals(schemaId);
    }

    @Override
    public RuleResult evaluate(Map<String, Object> payload) {
        Object value = payload.get(field);
        if (!(value instanceof Number)) {
            return RuleResult.builder()
                    .ruleId(ruleId()).field(field).severity(severity).passed(false)
                    .actualValue(String.valueOf(value))
                    .message(field + " is not a number: " + value)
                    .build();
        }
        double number = ((Number) value).doubleValue();
        boolean passed = number > 0;
        return RuleResult.builder()
                .ruleId(ruleId()).field(field).severity(severity).passed(passed)
                .actualValue(String.valueOf(number))
                .message(passed ? null : field + " must be > 0, got: " + number)
                .build();
    }
}
