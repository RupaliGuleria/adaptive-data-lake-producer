package com.adaptivedata.ingestion.model;

public enum EventResult {
    SUCCESS, DUPLICATE, RETRY_ROUTED, DLQ_ROUTED
}
