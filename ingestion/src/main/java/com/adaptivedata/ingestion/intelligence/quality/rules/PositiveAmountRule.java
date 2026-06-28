package com.adaptivedata.ingestion.intelligence.quality.rules;

import com.adaptivedata.ingestion.intelligence.quality.QualityRule;
import com.adaptivedata.ingestion.intelligence.quality.RuleResult;
import com.adaptivedata.ingestion.intelligence.quality.Severity;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class PositiveAmountRule implements QualityRule {

    @Override
    public String ruleId() {
        return "amount.positive";
    }

    @Override
    public RuleResult evaluate(Map<String, Object> payload) {
        Object value = payload.get("amount");
        if (!(value instanceof Number)) {
            return RuleResult.builder()
                    .ruleId(ruleId()).field("amount").severity(Severity.HIGH).passed(false)
                    .actualValue(String.valueOf(value))
                    .message("amount is not a number: " + value)
                    .build();
        }
        double amount = ((Number) value).doubleValue();
        boolean passed = amount > 0;
        return RuleResult.builder()
                .ruleId(ruleId()).field("amount").severity(Severity.HIGH).passed(passed)
                .actualValue(String.valueOf(amount))
                .message(passed ? null : "amount must be > 0, got: " + amount)
                .build();
    }
}
