package com.adaptivedata.ingestion.processor;

import com.adaptivedata.ingestion.config.IngestionConfig;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class DeduplicationStore {

    private final Cache<String, Boolean> cache;

    public DeduplicationStore(IngestionConfig config) {
        this.cache = Caffeine.newBuilder()
                .expireAfterWrite(config.getDedupCacheTtlMinutes(), TimeUnit.MINUTES)
                .build();
    }

    public boolean isDuplicate(String idempotencyKey) {
        return cache.getIfPresent(idempotencyKey) != null;
    }

    public void mark(String idempotencyKey) {
        cache.put(idempotencyKey, Boolean.TRUE);
    }
}
