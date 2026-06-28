package com.adaptivedata.ingestion.intelligence.quality;

import com.adaptivedata.ingestion.model.ProcessedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class QualityEngine {

    private static final Logger logger = LoggerFactory.getLogger(QualityEngine.class);

    private final List<QualityRule> rules;
    private final double threshold;

    public QualityEngine(
            List<QualityRule> rules,
            @Value("${ingestion.intelligence.quality.threshold:0.7}") double threshold) {
        this.rules = rules;
        this.threshold = threshold;
        logger.info("QualityEngine initialised | rules={} threshold={}", rules.size(), threshold);
    }

    public QualityCheckResult check(ProcessedEvent event) {
        Map<String, Object> payload = event.getPayload();

        List<RuleResult> results = rules.stream()
                .map(rule -> rule.evaluate(payload))
                .collect(Collectors.toList());

        // Any CRITICAL failure short-circuits — score drops to 0.0 immediately
        boolean criticalFailed = results.stream()
                .anyMatch(r -> r.getSeverity() == Severity.CRITICAL && !r.isPassed());

        double score;
        if (criticalFailed) {
            score = 0.0;
        } else {
            int totalWeight  = results.stream().mapToInt(r -> weight(r.getSeverity())).sum();
            int passedWeight = results.stream()
                    .filter(RuleResult::isPassed)
                    .mapToInt(r -> weight(r.getSeverity()))
                    .sum();
            score = totalWeight > 0 ? (double) passedWeight / totalWeight : 1.0;
        }

        boolean passed = score >= threshold;

        if (!passed) {
            List<String> failedRules = results.stream()
                    .filter(r -> !r.isPassed())
                    .map(RuleResult::getRuleId)
                    .collect(Collectors.toList());
            logger.warn("Quality check failed | event_id={} score={:.2f} threshold={} failed_rules={}",
                    event.getEventId(), score, threshold, failedRules);
        }

        return QualityCheckResult.builder()
                .qualityScore(score)
                .passed(passed)
                .threshold(threshold)
                .ruleResults(results)
                .build();
    }

    private int weight(Severity severity) {
        return switch (severity) {
            case CRITICAL -> 4;
            case HIGH     -> 3;
            case MEDIUM   -> 2;
            case LOW      -> 1;
        };
    }
}
