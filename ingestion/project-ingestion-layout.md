# Ingestion Module — Project Layout

```
ingestion/
├── DESIGN.md                                          Full design document
├── pom.xml                                            Spring Boot 3.2.5, Java 21
├── project-ingestion-layout.md                        This file
│
└── src/
    └── main/
        ├── resources/
        │   └── application.yml                        All runtime config (Kafka, topics, timeouts)
        │
        └── java/com/adaptivedata/ingestion/
            │
            ├── IngestionApplication.java              Entry point — @SpringBootApplication + @EnableScheduling
            │
            ├── config/
            │   ├── IngestionConfig.java               @ConfigurationProperties binding for ingestion.*
            │   ├── JacksonConfig.java                 Explicit ObjectMapper bean (base starter has no Jackson auto-config)
            │   ├── KafkaConsumerConfig.java           Batch listener factory, manual ack, concurrency
            │   └── KafkaProducerConfig.java           Idempotent KafkaTemplate for retry + DLQ topics
            │
            ├── model/
            │   ├── EventEnvelope.java                 Java mirror of Python EventEnvelope (snake_case via @JsonProperty)
            │   ├── TradeDocMessage.java               Java mirror of Python TradeDoc
            │   ├── ProcessedEvent.java                Clean normalized event passed to StagingBuffer
            │   ├── BatchProcessorResult.java          Aggregated per-poll outcome counts
            │   ├── BatchStatus.java                   Enum: PENDING / SUCCESS / FAIL
            │   └── EventResult.java                   Enum: SUCCESS / DUPLICATE / RETRY_ROUTED / DLQ_ROUTED
            │
            ├── consumer/
            │   ├── ControlDocListener.java            Batch listener on banking-control → BatchCoordinator.register()
            │   ├── EventBatchListener.java            Batch listener on banking-transactions → BatchProcessor
            │   └── RetryIngestionListener.java        Batch listener on retry topic; enforces backoff via Thread.sleep
            │
            ├── coordinator/
            │   ├── BatchCoordinator.java              Registers batches, tracks count + trade_id set, runs timeout check
            │   └── BatchState.java                    Per-trade-group state; tryComplete() uses CAS (AtomicBoolean)
            │
            ├── processor/
            │   ├── BatchProcessor.java                Splits poll across ExecutorService (8 workers, CompletableFuture)
            │   ├── DeduplicationStore.java            Caffeine cache keyed on idempotency_key (60 min TTL)
            │   └── ErrorClassifier.java               Maps exceptions to TRANSIENT or FATAL; unknown → FATAL
            │
            ├── staging/
            │   └── StagingBuffer.java                 LinkedBlockingQueue<ProcessedEvent>; drains every 5s or 1000 events
            │                                          Phase 2: swap drain target to MinIO, no other changes needed
            │
            └── routing/
                ├── DlqPublisher.java                  Routes per-event failures + batch timeouts to DLQ topic
                └── RetryPublisher.java                Stamps attempt_count + retry_after headers, publishes to retry topic
```

---

## Topic Wiring

```
banking-control              →  ControlDocListener   →  BatchCoordinator.register()
banking-transactions         →  EventBatchListener   →  BatchProcessor  →  BatchCoordinator.recordProcessed()
banking-transactions-retry   →  RetryIngestionListener → BatchProcessor (retry path)
banking-transactions-dlq     ←  DlqPublisher         (fatal errors, NO_CONTROL_DOC, retry exhausted, batch timeout)
```

---

## Key Design Invariants

| Invariant | Where enforced |
|---|---|
| Offset committed only after all events in poll are resolved | `EventBatchListener` — `ack.acknowledge()` in `finally` block |
| Batch SUCCESS requires count match AND trade_id set equality | `BatchState.tryComplete()` |
| SUCCESS transition fires exactly once per batch | `AtomicBoolean.compareAndSet` in `BatchState.tryComplete()` |
| Events reach StagingBuffer only on batch SUCCESS | `BatchCoordinator.recordProcessed()` |
| Unknown exceptions route to DLQ, not retry | `ErrorClassifier` — unknown → FATAL |
| Retry backoff enforced by retry consumer, not primary listener | `RetryIngestionListener.enforceBackoff()` |
