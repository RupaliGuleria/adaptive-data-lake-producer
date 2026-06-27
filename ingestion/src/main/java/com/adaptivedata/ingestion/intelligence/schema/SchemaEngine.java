package com.adaptivedata.ingestion.intelligence.schema;

import com.adaptivedata.ingestion.model.ProcessedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class SchemaEngine {

    private static final Logger logger = LoggerFactory.getLogger(SchemaEngine.class);

    private final Map<String, SchemaDefinition> registry = new HashMap<>();
    private final ObjectMapper objectMapper;

    public SchemaEngine(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void loadSchemas() throws Exception {
        Resource[] resources = new PathMatchingResourcePatternResolver()
                .getResources("classpath:schemas/*.json");
        for (Resource resource : resources) {
            SchemaDefinition def = objectMapper.readValue(resource.getInputStream(), SchemaDefinition.class);
            registry.put(def.getSchemaId(), def);
            logger.info("Schema loaded | id={} version={} fields={}",
                    def.getSchemaId(), def.getVersion(), def.getFields().size());
        }
    }

    public SchemaValidationResult validate(ProcessedEvent event) {
        SchemaDefinition schema = registry.get(event.getSchemaId());
        if (schema == null) {
            return unknownSchemaResult(event.getSchemaId());
        }

        Map<String, Object> payload = event.getPayload();
        Map<String, FieldDefinition> expectedFields = schema.getFields().stream()
                .collect(Collectors.toMap(FieldDefinition::getName, f -> f));

        List<SchemaViolation> violations = new ArrayList<>();

        // Required fields missing or type-mismatched
        for (FieldDefinition field : schema.getFields()) {
            Object value = payload.get(field.getName());
            if (value == null) {
                if (field.isRequired()) {
                    violations.add(SchemaViolation.builder()
                            .field(field.getName())
                            .expectedType(field.getType())
                            .actualType("null")
                            .violationType(DriftType.MISSING_FIELD)
                            .build());
                }
            } else {
                String actual = inferType(value);
                if (!actual.equals(field.getType())) {
                    violations.add(SchemaViolation.builder()
                            .field(field.getName())
                            .expectedType(field.getType())
                            .actualType(actual)
                            .violationType(DriftType.TYPE_CHANGE)
                            .build());
                }
            }
        }

        // Unknown fields not in schema — non-breaking drift
        for (String key : payload.keySet()) {
            if (!expectedFields.containsKey(key)) {
                violations.add(SchemaViolation.builder()
                        .field(key)
                        .expectedType("undefined")
                        .actualType(inferType(payload.get(key)))
                        .violationType(DriftType.NEW_FIELD)
                        .build());
            }
        }

        boolean driftDetected = !violations.isEmpty();
        boolean breakingChange = violations.stream().anyMatch(v ->
                v.getViolationType() == DriftType.TYPE_CHANGE ||
                v.getViolationType() == DriftType.MISSING_FIELD);

        Compatibility compatibility = deriveCompatibility(violations, breakingChange);
        RecommendedAction action = deriveAction(breakingChange, violations);

        DriftType dominantDrift = violations.stream()
                .map(SchemaViolation::getViolationType)
                .findFirst()
                .orElse(null);

        if (driftDetected) {
            logger.warn("Schema drift | event_id={} schema={} breaking={} violations={}",
                    event.getEventId(), event.getSchemaId(), breakingChange, violations.size());
        }

        return SchemaValidationResult.builder()
                .schemaId(event.getSchemaId())
                .schemaVersion(schema.getVersion())
                .valid(!breakingChange)
                .driftDetected(driftDetected)
                .driftType(dominantDrift)
                .breakingChange(breakingChange)
                .compatibility(compatibility)
                .recommendedAction(action)
                .violations(violations)
                .build();
    }

    private String inferType(Object value) {
        if (value instanceof String)  return "string";
        if (value instanceof Number)  return "number";
        if (value instanceof Boolean) return "boolean";
        return "unknown";
    }

    private Compatibility deriveCompatibility(List<SchemaViolation> violations, boolean breakingChange) {
        if (violations.isEmpty()) return Compatibility.FULL;
        if (breakingChange)       return Compatibility.NONE;
        boolean onlyNewFields = violations.stream()
                .allMatch(v -> v.getViolationType() == DriftType.NEW_FIELD);
        return onlyNewFields ? Compatibility.BACKWARD : Compatibility.FORWARD;
    }

    private RecommendedAction deriveAction(boolean breakingChange, List<SchemaViolation> violations) {
        if (violations.isEmpty()) return RecommendedAction.AUTO_EVOLVE;
        if (breakingChange)       return RecommendedAction.REJECT;
        boolean onlyNewFields = violations.stream()
                .allMatch(v -> v.getViolationType() == DriftType.NEW_FIELD);
        return onlyNewFields ? RecommendedAction.AUTO_EVOLVE : RecommendedAction.QUARANTINE;
    }

    private SchemaValidationResult unknownSchemaResult(String schemaId) {
        logger.error("Unknown schema | schema_id={}", schemaId);
        return SchemaValidationResult.builder()
                .schemaId(schemaId)
                .schemaVersion("unknown")
                .valid(false)
                .driftDetected(true)
                .driftType(DriftType.MISSING_FIELD)
                .breakingChange(true)
                .compatibility(Compatibility.NONE)
                .recommendedAction(RecommendedAction.REJECT)
                .violations(List.of(SchemaViolation.builder()
                        .field("schema_id")
                        .expectedType("registered schema")
                        .actualType("unknown")
                        .violationType(DriftType.MISSING_FIELD)
                        .build()))
                .build();
    }
}
