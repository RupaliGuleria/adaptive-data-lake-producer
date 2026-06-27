package com.adaptivedata.ingestion.intelligence.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class SchemaDefinition {

    @JsonProperty("schema_id")
    private String schemaId;

    private String version;

    private List<FieldDefinition> fields;
}
