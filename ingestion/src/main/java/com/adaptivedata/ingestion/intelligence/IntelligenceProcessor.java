package com.adaptivedata.ingestion.intelligence;

import com.adaptivedata.ingestion.intelligence.model.ValidatedEvent;
import com.adaptivedata.ingestion.intelligence.model.ValidationStatus;
import com.adaptivedata.ingestion.intelligence.quality.QualityCheckResult;
import com.adaptivedata.ingestion.intelligence.quality.QualityEngine;
import com.adaptivedata.ingestion.intelligence.routing.IntelligenceRouter;
import com.adaptivedata.ingestion.intelligence.schema.SchemaEngine;
import com.adaptivedata.ingestion.intelligence.schema.SchemaValidationResult;
import com.adaptivedata.ingestion.model.ProcessedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class IntelligenceProcessor {

    private static final Logger logger = LoggerFactory.getLogger(IntelligenceProcessor.class);

    private final SchemaEngine schemaEngine;
    private final QualityEngine qualityEngine;
    private final IntelligenceRouter router;

    public IntelligenceProcessor(SchemaEngine schemaEngine, QualityEngine qualityEngine, IntelligenceRouter router) {
        this.schemaEngine = schemaEngine;
        this.qualityEngine = qualityEngine;
        this.router = router;
    }

    public void processBatch(List<ProcessedEvent> events) {
        if (events.isEmpty()) return;

        List<ValidatedEvent> validated = events.stream()
                .map(this::validate)
                .collect(Collectors.toList());

        validated.forEach(router::route);
        logBatchReport(validated);
    }

    private ValidatedEvent validate(ProcessedEvent event) {
        SchemaValidationResult schemaResult = schemaEngine.validate(event);

        QualityCheckResult qualityResult;
        ValidationStatus status;

        if (schemaResult.isBreakingChange()) {
            // Breaking schema change — skip quality check, reject immediately
            qualityResult = skippedQuality();
            status = ValidationStatus.REJECTED;
        } else {
            qualityResult = qualityEngine.check(event);
            status = deriveStatus(qualityResult.getQualityScore());
        }

        return ValidatedEvent.builder()
                .event(event)
                .schemaResult(schemaResult)
                .qualityResult(qualityResult)
                .validationStatus(status)
                .ingestionTime(Instant.now().toString())
                .pipelineVersion(event.getPipelineVersion())
                .sourceTopic("banking-transactions")
                .build();
    }

    private ValidationStatus deriveStatus(double score) {
        if (score >= 0.7) return ValidationStatus.PASSED;
        if (score >= 0.5) return ValidationStatus.QUARANTINED;
        return ValidationStatus.REJECTED;
    }

    private QualityCheckResult skippedQuality() {
        return QualityCheckResult.builder()
                .qualityScore(0.0)
                .passed(false)
                .threshold(0.0)
                .ruleResults(List.of())
                .build();
    }

    private void logBatchReport(List<ValidatedEvent> events) {
        int total       = events.size();
        long passed      = count(events, ValidationStatus.PASSED);
        long quarantined = count(events, ValidationStatus.QUARANTINED);
        long rejected    = count(events, ValidationStatus.REJECTED);
        long driftCount  = events.stream().filter(e -> e.getSchemaResult().isDriftDetected()).count();
        double avgScore  = events.stream()
                .mapToDouble(e -> e.getQualityResult().getQualityScore())
                .average()
                .orElse(0.0);

        logger.info(
            "Intelligence Batch Report | total={} passed={}({}%) quarantined={}({}%) rejected={}({}%) schema_drift={} avg_quality_score={}",
            total,
            passed,      pct(passed, total),
            quarantined, pct(quarantined, total),
            rejected,    pct(rejected, total),
            driftCount,
            String.format("%.2f", avgScore)
        );

        // Log detail for any deviations so they're visible without a dashboard
        if (driftCount > 0) {
            events.stream()
                    .filter(e -> e.getSchemaResult().isDriftDetected())
                    .forEach(e -> logger.warn(
                            "Schema deviation | event_id={} drift_type={} breaking={} violations={}",
                            e.getEvent().getEventId(),
                            e.getSchemaResult().getDriftType(),
                            e.getSchemaResult().isBreakingChange(),
                            e.getSchemaResult().getViolations().size()));
        }

        if (rejected > 0 || quarantined > 0) {
            events.stream()
                    .filter(e -> e.getValidationStatus() != ValidationStatus.PASSED)
                    .forEach(e -> {
                        List<String> failedRules = e.getQualityResult().getRuleResults().stream()
                                .filter(r -> !r.isPassed())
                                .map(r -> r.getRuleId() + "[" + r.getSeverity() + "]")
                                .collect(Collectors.toList());
                        if (!failedRules.isEmpty()) {
                            logger.warn("Quality deviation | event_id={} status={} score={} failed_rules={}",
                                    e.getEvent().getEventId(),
                                    e.getValidationStatus(),
                                    String.format("%.2f", e.getQualityResult().getQualityScore()),
                                    failedRules);
                        }
                    });
        }
    }

    private long count(List<ValidatedEvent> events, ValidationStatus status) {
        return events.stream().filter(e -> e.getValidationStatus() == status).count();
    }

    private String pct(long part, int total) {
        return String.format("%.1f", total > 0 ? (double) part / total * 100 : 0.0);
    }
}
