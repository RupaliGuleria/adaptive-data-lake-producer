package com.adaptivedata.ingestion.intelligence.quality.rules;

import com.adaptivedata.ingestion.intelligence.quality.QualityRule;
import com.adaptivedata.ingestion.intelligence.quality.RuleResult;
import com.adaptivedata.ingestion.intelligence.quality.Severity;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;

@Component
public class ValidStatusRule implements QualityRule {

    private static final Set<String> VALID = Set.of("Success", "Failed", "Pending");

    @Override
    public String ruleId() {
        return "transaction_status.valid_value";
    }

    @Override
    public boolean appliesTo(String schemaId) {
        return "banking_transaction_v1".equals(schemaId);
    }

    @Override
    public RuleResult evaluate(Map<String, Object> payload) {
        Object value = payload.get("transaction_status");
        String status = value != null ? value.toString() : null;
        boolean passed = status != null && VALID.contains(status);
        return RuleResult.builder()
                .ruleId(ruleId()).field("transaction_status").severity(Severity.MEDIUM).passed(passed)
                .actualValue(status != null ? status : "null")
                .message(passed ? null : "transaction_status must be one of " + VALID + ", got: " + status)
                .build();
    }
}
