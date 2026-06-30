package com.adaptivedata.ingestion.intelligence.storage;

import com.adaptivedata.ingestion.config.MinioProperties;
import com.adaptivedata.ingestion.intelligence.model.ValidatedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

@Component
public class MinioStorageWriter {

    private static final Logger logger = LoggerFactory.getLogger(MinioStorageWriter.class);

    private final S3Client s3Client;
    private final String bucket;
    private final ObjectMapper objectMapper;

    public MinioStorageWriter(S3Client s3Client, MinioProperties props, ObjectMapper objectMapper) {
        this.s3Client = s3Client;
        this.bucket = props.getBucket();
        this.objectMapper = objectMapper;
    }

    /**
     * Writes a validated event to MinIO under the given prefix.
     * Key pattern: {prefix}/year=YYYY/month=MM/day=DD/hour=HH/source={src}/{event_id}.json
     * Hive-style partitioning makes the data immediately queryable by DuckDB/Spark (Phase 4).
     */
    public void write(String prefix, ValidatedEvent event) {
        String key = buildKey(prefix, event);
        try {
            String json = objectMapper.writeValueAsString(event);
            s3Client.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .contentType("application/json")
                            .build(),
                    RequestBody.fromString(json, StandardCharsets.UTF_8)
            );
            logger.debug("Written to MinIO | bucket={} key={}", bucket, key);
        } catch (Exception e) {
            logger.error("MinIO write failed | key={} event_id={}",
                    key, event.getEvent().getEventId(), e);
        }
    }

    private String buildKey(String prefix, ValidatedEvent event) {
        ZonedDateTime zdt = Instant.parse(event.getIngestionTime()).atZone(ZoneOffset.UTC);
        return String.format("%s/year=%d/month=%02d/day=%02d/hour=%02d/source=%s/%s.json",
                prefix.replaceAll("^/|/$", ""),
                zdt.getYear(),
                zdt.getMonthValue(),
                zdt.getDayOfMonth(),
                zdt.getHour(),
                event.getEvent().getSource(),
                event.getEvent().getEventId());
    }
}
