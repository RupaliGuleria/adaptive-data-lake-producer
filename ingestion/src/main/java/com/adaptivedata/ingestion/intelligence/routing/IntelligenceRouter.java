package com.adaptivedata.ingestion.intelligence.routing;

import com.adaptivedata.ingestion.intelligence.model.ValidatedEvent;
import com.adaptivedata.ingestion.intelligence.storage.ParquetWriteBuffer;
import com.adaptivedata.ingestion.routing.DlqPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class IntelligenceRouter {

    private static final Logger logger = LoggerFactory.getLogger(IntelligenceRouter.class);

    private final ParquetWriteBuffer parquetWriteBuffer;
    private final DlqPublisher dlqPublisher;

    public IntelligenceRouter(ParquetWriteBuffer parquetWriteBuffer, DlqPublisher dlqPublisher) {
        this.parquetWriteBuffer = parquetWriteBuffer;
        this.dlqPublisher = dlqPublisher;
    }

    public void route(ValidatedEvent validated) {
        switch (validated.getValidationStatus()) {
            case PASSED      -> handlePassed(validated);
            case QUARANTINED -> handleQuarantined(validated);
            case REJECTED    -> handleRejected(validated);
        }
    }

    private void handlePassed(ValidatedEvent validated) {
        parquetWriteBuffer.add("data", validated);
        logger.info("PASSED → Parquet buffer /data/ | event_id={} schema_version={} quality_score={}",
                validated.getEvent().getEventId(),
                validated.getSchemaResult().getSchemaVersion(),
                String.format("%.2f", validated.getQualityResult().getQualityScore()));
    }

    private void handleQuarantined(ValidatedEvent validated) {
        parquetWriteBuffer.add("quarantine", validated);
        logger.warn("QUARANTINED → Parquet buffer /quarantine/ | event_id={} quality_score={} drift={} drift_type={}",
                validated.getEvent().getEventId(),
                String.format("%.2f", validated.getQualityResult().getQualityScore()),
                validated.getSchemaResult().isDriftDetected(),
                validated.getSchemaResult().getDriftType());
    }

    private void handleRejected(ValidatedEvent validated) {
        String reason = buildRejectionReason(validated);
        dlqPublisher.publishRejected(validated.getEvent(), reason);
        logger.error("REJECTED → DLQ | event_id={} reason={}",
                validated.getEvent().getEventId(), reason);
    }

    private String buildRejectionReason(ValidatedEvent validated) {
        if (validated.getSchemaResult().isBreakingChange()) {
            return "SCHEMA_BREAKING_CHANGE:" + validated.getSchemaResult().getDriftType();
        }
        return String.format("QUALITY_REJECTED:score=%.2f", validated.getQualityResult().getQualityScore());
    }
}
