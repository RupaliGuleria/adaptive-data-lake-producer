package com.adaptivedata.ingestion.intelligence.storage;

import com.adaptivedata.ingestion.config.MinioProperties;
import com.adaptivedata.ingestion.intelligence.model.ValidatedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Converts a buffered batch of {@link ValidatedEvent}s into a single Parquet file and
 * uploads it to MinIO. Writes to a local temp file first (Parquet needs a seekable,
 * Hadoop-{@code OutputFile}-compatible target) then ships the finished file with a single
 * {@code PutObject} call — the object only appears in the bucket once it's complete, so
 * partition reads never see a partially-written file.
 */
@Component
public class ParquetStorageWriter {

    private static final Logger logger = LoggerFactory.getLogger(ParquetStorageWriter.class);
    private static final String SCHEMA_RESOURCE = "/avro/banking_transaction_record.avsc";

    private final S3Client s3Client;
    private final String bucket;
    private final Schema avroSchema;
    private final ParquetRecordMapper recordMapper;
    private final AtomicLong fileCounter = new AtomicLong();

    public ParquetStorageWriter(S3Client s3Client, MinioProperties props, ObjectMapper objectMapper) {
        this.s3Client = s3Client;
        this.bucket = props.getBucket();
        this.avroSchema = loadSchema();
        this.recordMapper = new ParquetRecordMapper(objectMapper);
    }

    private Schema loadSchema() {
        try (InputStream in = getClass().getResourceAsStream(SCHEMA_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("Avro schema not found on classpath: " + SCHEMA_RESOURCE);
            }
            return new Schema.Parser().parse(in);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load Avro schema " + SCHEMA_RESOURCE, e);
        }
    }

    /**
     * Writes all events into one Parquet file under {@code partitionKey} and uploads it to MinIO.
     * @param partitionKey Hive-style path, e.g. {@code data/year=2026/month=07/day=01/hour=14/source=cloud}
     */
    public void writeBatch(String partitionKey, List<ValidatedEvent> events) {
        if (events.isEmpty()) {
            return;
        }

        java.nio.file.Path tempNioPath;
        try {
            tempNioPath = Files.createTempFile("parquet-", ".parquet");
        } catch (IOException e) {
            logger.error("Failed to allocate temp file for Parquet write | partition={} events={}",
                    partitionKey, events.size(), e);
            return;
        }

        try {
            try (org.apache.parquet.hadoop.ParquetWriter<GenericRecord> writer = AvroParquetWriter
                    .<GenericRecord>builder(new NioLocalOutputFile(tempNioPath))
                    .withSchema(avroSchema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withDictionaryEncoding(true)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .build()) {
                for (ValidatedEvent event : events) {
                    writer.write(recordMapper.toRecord(avroSchema, event));
                }
            }

            String key = buildKey(partitionKey);
            long sizeBytes = Files.size(tempNioPath);
            s3Client.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .contentType("application/octet-stream")
                            .build(),
                    RequestBody.fromFile(tempNioPath));

            logger.info("Parquet written → MinIO | bucket={} key={} events={} size_bytes={}",
                    bucket, key, events.size(), sizeBytes);
        } catch (IOException e) {
            logger.error("Parquet write failed | partition={} events={}", partitionKey, events.size(), e);
        } finally {
            try {
                Files.deleteIfExists(tempNioPath);
            } catch (IOException ignored) {
                // best-effort cleanup of the local temp file
            }
        }
    }

    private String buildKey(String partitionKey) {
        return String.format("%s/part-%d-%d.parquet",
                partitionKey, fileCounter.incrementAndGet(), System.currentTimeMillis());
    }
}
