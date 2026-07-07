package com.adaptivedata.ingestion.query;

/** Common contract for DuckDB and Spark query adapters. */
public interface QueryEngine {

    QueryResult execute(QueryFilter filter);

    /** Short identifier logged in QueryResult.engine and used by the router. */
    String engineName();
}
