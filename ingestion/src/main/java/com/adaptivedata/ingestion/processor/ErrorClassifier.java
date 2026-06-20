package com.adaptivedata.ingestion.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;

@Component
public class ErrorClassifier {

    public ErrorType classify(Exception e) {
        if (e instanceof JsonProcessingException
                || e instanceof NullPointerException
                || e instanceof IllegalArgumentException
                || e instanceof ClassCastException) {
            return ErrorType.FATAL;
        }
        if (e instanceof ConnectException
                || e instanceof SocketTimeoutException
                || e instanceof IOException) {
            return ErrorType.TRANSIENT;
        }
        // Unknown exceptions default to FATAL to avoid infinite retry loops
        return ErrorType.FATAL;
    }

    public enum ErrorType {
        TRANSIENT, FATAL
    }
}
