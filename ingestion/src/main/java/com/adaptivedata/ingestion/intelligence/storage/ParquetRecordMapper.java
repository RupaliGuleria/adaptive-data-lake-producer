package com.adaptivedata.ingestion.intelligence.storage;

import com.adaptivedata.ingestion.intelligence.model.ValidatedEvent;
import com.adaptivedata.ingestion.intelligence.quality.QualityCheckResult;
import com.adaptivedata.ingestion.intelligence.schema.SchemaValidationResult;
import com.adaptivedata.ingestion.model.ProcessedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/** Flattens a {@link ValidatedEvent} into a {@link GenericRecord} matching banking_transaction_record.avsc. */
final class ParquetRecordMapper {

    private final ObjectMapper objectMapper;

    ParquetRecordMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    GenericRecord toRecord(Schema avroSchema, ValidatedEvent validated) {
        ProcessedEvent event = validated.getEvent();
        SchemaValidationResult schemaResult = validated.getSchemaResult();
        QualityCheckResult qualityResult = validated.getQualityResult();

        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("event_id", event.getEventId());
        record.put("trade_group_id", event.getTradeGroupId());
        record.put("trade_id", event.getTradeId());
        record.put("idempotency_key", event.getIdempotencyKey());
        record.put("event_type", event.getEventType());
        record.put("timestamp", event.getTimestamp());
        record.put("source", event.getSource());
        record.put("pipeline_version", event.getPipelineVersion());
        record.put("schema_id", event.getSchemaId());
        record.put("payload_json", toJson(event.getPayload()));

        record.put("validation_status", validated.getValidationStatus().name());
        record.put("schema_version", schemaResult.getSchemaVersion());
        record.put("schema_valid", schemaResult.isValid());
        record.put("drift_detected", schemaResult.isDriftDetected());
        record.put("drift_type", schemaResult.getDriftType() != null ? schemaResult.getDriftType().name() : null);
        record.put("breaking_change", schemaResult.isBreakingChange());
        record.put("compatibility", schemaResult.getCompatibility() != null ? schemaResult.getCompatibility().name() : null);
        record.put("schema_violations_json", toJson(schemaResult.getViolations()));

        record.put("quality_score", qualityResult.getQualityScore());
        record.put("quality_passed", qualityResult.isPassed());
        record.put("quality_threshold", qualityResult.getThreshold());
        record.put("quality_rule_results_json", toJson(qualityResult.getRuleResults()));

        record.put("ingestion_time", validated.getIngestionTime());
        record.put("source_topic", validated.getSourceTopic());
        return record;
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception e) {
            return "{}";
        }
    }
}
