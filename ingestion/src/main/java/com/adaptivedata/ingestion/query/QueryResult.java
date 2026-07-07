package com.adaptivedata.ingestion.query;

import lombok.Builder;
import lombok.Value;

import java.util.List;
import java.util.Map;

/** Standardised result returned by any QueryEngine implementation. */
@Value
@Builder
public class QueryResult {

    /** Column names in the order they appear in each row map. */
    List<String> columns;

    /** Each element is one row: column name → value (nullable). */
    List<Map<String, Object>> rows;

    int totalRows;
    long executionTimeMs;

    /** Which engine produced this result: "duckdb" or "spark". */
    String engine;

    /** The S3 path glob that was scanned. */
    String scannedPath;
}
