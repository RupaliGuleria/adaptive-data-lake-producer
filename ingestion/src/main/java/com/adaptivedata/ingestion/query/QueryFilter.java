package com.adaptivedata.ingestion.query;

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
    String prefix = "data";

    /** Hive partition columns — set progressively for narrower scans. */
    Integer year;
    Integer month;
    Integer day;
    Integer hour;
    String  source;

    /**
     * Optional extra SQL WHERE predicate appended after any partition conditions.
     * Example: {@code "quality_score < 0.8 AND validation_status = 'QUARANTINED'"}
     * Injected as-is — callers are responsible for safe values (no user input here in Phase 7).
     */
    String additionalWhere;

    /** Maximum rows returned — safety cap to prevent OOM on large scans. */
    @Builder.Default
    int maxRows = 10_000;

    /** When true the router will prefer Spark over DuckDB for this query. */
    @Builder.Default
    boolean preferSpark = false;
}
