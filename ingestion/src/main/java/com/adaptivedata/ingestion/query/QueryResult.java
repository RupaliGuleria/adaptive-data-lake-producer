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

    /** Total rows matching the filter across all pages (from a COUNT(*) query). */
    int totalRows;

    /** Rows actually returned in this response, i.e. {@code rows.size()}. */
    int returnedRows;

    /** 1-based page number this result represents. */
    int page;

    /** Rows requested per page. */
    int pageSize;

    /** {@code ceil(totalRows / pageSize)}, minimum 1. */
    int totalPages;

    long executionTimeMs;

    /** Which engine produced this result: "duckdb" or "spark". */
    String engine;

    /** The S3 path glob that was scanned. */
    String scannedPath;
}
