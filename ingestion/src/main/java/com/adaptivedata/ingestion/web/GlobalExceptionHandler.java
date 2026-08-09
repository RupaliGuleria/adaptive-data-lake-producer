package com.adaptivedata.ingestion.web;

import com.adaptivedata.ingestion.query.QueryExecutionException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.util.List;

/** Maps every exception thrown by REST controllers onto a standardised {@link ApiError} body. */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiError> handleBodyValidation(MethodArgumentNotValidException e, HttpServletRequest req) {
        List<String> details = e.getBindingResult().getFieldErrors().stream()
                .map(fe -> fe.getField() + ": " + fe.getDefaultMessage())
                .toList();
        logger.warn("Request validation failed | path={} details={}", req.getRequestURI(), details);
        return ResponseEntity.badRequest()
                .body(ApiError.of("VALIDATION_ERROR", "Request body failed validation", req.getRequestURI(), details));
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<ApiError> handleConstraintViolation(ConstraintViolationException e, HttpServletRequest req) {
        List<String> details = e.getConstraintViolations().stream()
                .map(v -> v.getPropertyPath() + ": " + v.getMessage())
                .toList();
        logger.warn("Request parameter validation failed | path={} details={}", req.getRequestURI(), details);
        return ResponseEntity.badRequest()
                .body(ApiError.of("VALIDATION_ERROR", "Request parameters failed validation", req.getRequestURI(), details));
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ResponseEntity<ApiError> handleMissingParam(MissingServletRequestParameterException e, HttpServletRequest req) {
        return ResponseEntity.badRequest()
                .body(ApiError.of("MISSING_PARAMETER", e.getMessage(), req.getRequestURI()));
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ApiError> handleTypeMismatch(MethodArgumentTypeMismatchException e, HttpServletRequest req) {
        Class<?> requiredType = e.getRequiredType();
        String message = "Parameter '" + e.getName() + "' must be of type "
                + (requiredType != null ? requiredType.getSimpleName() : "unknown");
        return ResponseEntity.badRequest()
                .body(ApiError.of("TYPE_MISMATCH", message, req.getRequestURI()));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiError> handleMalformedBody(HttpMessageNotReadableException e, HttpServletRequest req) {
        return ResponseEntity.badRequest()
                .body(ApiError.of("MALFORMED_REQUEST", "Request body is missing or malformed JSON", req.getRequestURI()));
    }

    @ExceptionHandler(QueryExecutionException.class)
    public ResponseEntity<ApiError> handleQueryExecution(QueryExecutionException e, HttpServletRequest req) {
        logger.error("Query execution failed | path={}", req.getRequestURI(), e);
        return ResponseEntity.internalServerError()
                .body(ApiError.of("QUERY_EXECUTION_ERROR", e.getMessage(), req.getRequestURI()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> handleUnexpected(Exception e, HttpServletRequest req) {
        logger.error("Unhandled error | path={}", req.getRequestURI(), e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiError.of("INTERNAL_ERROR", "An unexpected error occurred", req.getRequestURI()));
    }
}
