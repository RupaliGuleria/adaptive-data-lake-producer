package com.adaptivedata.ingestion.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class BatchProcessorResult {
    private int total;
    private int success;
    private int duplicates;
    private int retryRouted;
    private int dlqRouted;

    public static BatchProcessorResult from(List<EventResult> results) {
        int success = 0, duplicates = 0, retry = 0, dlq = 0;
        for (EventResult r : results) {
            switch (r) {
                case SUCCESS      -> success++;
                case DUPLICATE    -> duplicates++;
                case RETRY_ROUTED -> retry++;
                case DLQ_ROUTED   -> dlq++;
            }
        }
        return BatchProcessorResult.builder()
                .total(results.size())
                .success(success)
                .duplicates(duplicates)
                .retryRouted(retry)
                .dlqRouted(dlq)
                .build();
    }
}
