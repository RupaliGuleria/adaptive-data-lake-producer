package com.adaptivedata.ingestion.intelligence.quality.rules;

import com.adaptivedata.ingestion.intelligence.quality.QualityRule;
import com.adaptivedata.ingestion.intelligence.quality.RuleResult;
import com.adaptivedata.ingestion.intelligence.quality.Severity;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class CreditScoreRangeRule implements QualityRule {

    private static final int MIN = 300;
    private static final int MAX = 900;

    @Override
    public String ruleId() {
        return "credit_score.in_range";
    }

    @Override
    public RuleResult evaluate(Map<String, Object> payload) {
        Object value = payload.get("credit_score");
        // credit_score is optional — skip without penalising the score
        if (value == null) {
            return RuleResult.builder()
                    .ruleId(ruleId()).field("credit_score").severity(Severity.LOW).passed(true)
                    .actualValue("null").message("credit_score absent — skipped")
                    .build();
        }
        if (!(value instanceof Number)) {
            return RuleResult.builder()
                    .ruleId(ruleId()).field("credit_score").severity(Severity.LOW).passed(false)
                    .actualValue(String.valueOf(value))
                    .message("credit_score is not a number: " + value)
                    .build();
        }
        int score = ((Number) value).intValue();
        boolean passed = score >= MIN && score <= MAX;
        return RuleResult.builder()
                .ruleId(ruleId()).field("credit_score").severity(Severity.LOW).passed(passed)
                .actualValue(String.valueOf(score))
                .message(passed ? null : "credit_score must be " + MIN + "–" + MAX + ", got: " + score)
                .build();
    }
}
