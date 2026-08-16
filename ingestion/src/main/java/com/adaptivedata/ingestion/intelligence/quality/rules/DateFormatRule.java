package com.adaptivedata.ingestion.intelligence.quality.rules;

import com.adaptivedata.ingestion.intelligence.quality.QualityRule;
import com.adaptivedata.ingestion.intelligence.quality.RuleResult;
import com.adaptivedata.ingestion.intelligence.quality.Severity;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;

@Component
public class DateFormatRule implements QualityRule {

    private static final DateTimeFormatter ISO_DATE = DateTimeFormatter.ISO_LOCAL_DATE;

    @Override
    public String ruleId() {
        return "transaction_date.iso_format";
    }

    @Override
    public boolean appliesTo(String schemaId) {
        return "banking_transaction_v1".equals(schemaId);
    }

    @Override
    public RuleResult evaluate(Map<String, Object> payload) {
        Object value = payload.get("transaction_date");
        String raw = value != null ? value.toString() : null;
        boolean passed = false;
        if (raw != null) {
            try {
                LocalDate.parse(raw, ISO_DATE);
                passed = true;
            } catch (DateTimeParseException ignored) {
            }
        }
        return RuleResult.builder()
                .ruleId(ruleId()).field("transaction_date").severity(Severity.HIGH).passed(passed)
                .actualValue(raw != null ? raw : "null")
                .message(passed ? null : "transaction_date must be yyyy-MM-dd, got: " + raw)
                .build();
    }
}
