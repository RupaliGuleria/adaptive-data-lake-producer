package com.adaptivedata.ingestion.query;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * Describes what to query and which partitions to target.
 * Partition fields (year → hour → source) that are set get embedded directly into the
 * S3 glob path — DuckDB prunes at the directory level before reading any data.
 * Fields left null widen the scan to all values for that partition level.
 */
@Value
@Builder
@Jacksonized
public class QueryFilter {

    /** MinIO prefix to scan: "data" for PASSED events, "quarantine" for QUARANTINED. */
    @Builder.Default
    @NotBlank(message = "prefix must not be blank")
    String prefix = "data";

    /** Hive partition columns — set progressively for narrower scans. */
    @Min(value = 2000, message = "year must be >= 2000")
    @Max(value = 2100, message = "year must be <= 2100")
    Integer year;

    @Min(value = 1, message = "month must be between 1 and 12")
    @Max(value = 12, message = "month must be between 1 and 12")
    Integer month;

    @Min(value = 1, message = "day must be between 1 and 31")
    @Max(value = 31, message = "day must be between 1 and 31")
    Integer day;

    @Min(value = 0, message = "hour must be between 0 and 23")
    @Max(value = 23, message = "hour must be between 0 and 23")
    Integer hour;

    String source;

    /**
     * Optional extra SQL WHERE predicate appended after any partition conditions.
     * Example: {@code "quality_score < 0.8 AND validation_status = 'QUARANTINED'"}
     * Injected as-is — callers are responsible for safe values (no user input here in Phase 7).
     */
    String additionalWhere;

    /** Maximum rows returned — safety cap to prevent OOM on large scans. */
    @Builder.Default
    @Min(value = 1, message = "maxRows must be >= 1")
    @Max(value = 100_000, message = "maxRows must be <= 100000")
    int maxRows = 10_000;

    /** 1-based page number. */
    @Builder.Default
    @Min(value = 1, message = "page must be >= 1")
    int page = 1;

    /** Rows per page — combines with {@code page} to compute the SQL OFFSET/LIMIT. */
    @Builder.Default
    @Min(value = 1, message = "pageSize must be >= 1")
    @Max(value = 10_000, message = "pageSize must be <= 10000")
    int pageSize = 100;

    /** When true the router will prefer Spark over DuckDB for this query. */
    @Builder.Default
    boolean preferSpark = false;
}
