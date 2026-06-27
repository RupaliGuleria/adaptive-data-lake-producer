package com.adaptivedata.ingestion.intelligence.schema;

import lombok.Builder;
import lombok.Value;

import java.util.List;

@Value
@Builder
public class SchemaValidationResult {
    String schemaId;
    String schemaVersion;

    /** True when no breaking changes are present — event can proceed to quality check. */
    boolean valid;

    boolean driftDetected;

    /** Dominant drift type when multiple violations exist; null when no drift. */
    DriftType driftType;

    boolean breakingChange;
    Compatibility compatibility;
    RecommendedAction recommendedAction;
    List<SchemaViolation> violations;
}
