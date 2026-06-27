package com.adaptivedata.ingestion.intelligence.schema;

import lombok.Data;

@Data
public class FieldDefinition {
    private String name;
    private String type;
    private boolean required;
}
