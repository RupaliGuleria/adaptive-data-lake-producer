package com.adaptivedata.ingestion.web;

import java.time.Instant;
import java.util.List;

/** Standardised error body returned by every REST endpoint on failure. */
public record ApiError(
        String code,
        String message,
        String path,
        Instant timestamp,
        List<String> details) {

    public static ApiError of(String code, String message, String path) {
        return new ApiError(code, message, path, Instant.now(), List.of());
    }

    public static ApiError of(String code, String message, String path, List<String> details) {
        return new ApiError(code, message, path, Instant.now(), details);
    }
}
