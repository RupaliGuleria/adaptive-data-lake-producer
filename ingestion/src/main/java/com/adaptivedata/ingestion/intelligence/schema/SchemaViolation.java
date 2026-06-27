package com.adaptivedata.ingestion.intelligence.schema;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class SchemaViolation {
    String field;
    String expectedType;
    String actualType;
    DriftType violationType;
}
