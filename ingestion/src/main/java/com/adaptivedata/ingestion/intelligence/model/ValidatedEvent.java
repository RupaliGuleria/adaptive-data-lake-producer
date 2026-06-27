package com.adaptivedata.ingestion.intelligence.model;

import com.adaptivedata.ingestion.intelligence.quality.QualityCheckResult;
import com.adaptivedata.ingestion.intelligence.schema.SchemaValidationResult;
import com.adaptivedata.ingestion.model.ProcessedEvent;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class ValidatedEvent {
    ProcessedEvent event;
    SchemaValidationResult schemaResult;
    QualityCheckResult qualityResult;
    ValidationStatus validationStatus;
    String ingestionTime;
    String pipelineVersion;
    String sourceTopic;
}
