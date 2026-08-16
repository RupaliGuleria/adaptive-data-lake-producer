package com.adaptivedata.ingestion.intelligence.quality.rules;

import com.adaptivedata.ingestion.intelligence.quality.QualityRule;
import com.adaptivedata.ingestion.intelligence.quality.RuleResult;
import com.adaptivedata.ingestion.intelligence.quality.Severity;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class NonNegativeBalanceRule implements QualityRule {

    @Override
    public String ruleId() {
        return "balance.non_negative";
    }

    @Override
    public boolean appliesTo(String schemaId) {
        return "banking_transaction_v1".equals(schemaId);
    }

    @Override
    public RuleResult evaluate(Map<String, Object> payload) {
        Object value = payload.get("balance");
        // balance is optional — skip without penalising the score
        if (value == null) {
            return RuleResult.builder()
                    .ruleId(ruleId()).field("balance").severity(Severity.MEDIUM).passed(true)
                    .actualValue("null").message("balance absent — skipped")
                    .build();
        }
        if (!(value instanceof Number)) {
            return RuleResult.builder()
                    .ruleId(ruleId()).field("balance").severity(Severity.MEDIUM).passed(false)
                    .actualValue(String.valueOf(value))
                    .message("balance is not a number: " + value)
                    .build();
        }
        double balance = ((Number) value).doubleValue();
        boolean passed = balance >= 0;
        return RuleResult.builder()
                .ruleId(ruleId()).field("balance").severity(Severity.MEDIUM).passed(passed)
                .actualValue(String.valueOf(balance))
                .message(passed ? null : "balance must be >= 0, got: " + balance)
                .build();
    }
}
