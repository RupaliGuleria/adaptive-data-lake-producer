# Adaptive Data Lake — Pipeline Flow

> Living document. Updated as each layer is built.
> For architectural decisions and trade-offs see `architecture.md`.
> For ingestion internals see `ingestion/DESIGN.md`.

---

## Current Build Status

| Layer | Status | Technology |
|---|---|---|
| Producer | ✅ Built | Python, confluent-kafka, Pydantic |
| Kafka Infrastructure | ✅ Built | Docker Compose, Confluent Platform 7.6 |
| Ingestion (on-prem) | ✅ Built | Java 21, Spring Boot 3.2, Spring Kafka |
| Intelligence Layer | ✅ Built | Java 21, Spring Boot (same app) |
| Storage — MinIO | ✅ Built | MinIO (S3-compatible), AWS SDK v2, Docker Compose |
| Parquet Writer | 🔲 Phase 3 | Apache Parquet |
| Query Interface | 🔲 Phase 4 | DuckDB / Spark |
| REST API | 🔲 Phase 5 | Spring Boot |

---

## End-to-End Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│  PRODUCER  (producers/cloud/)                                       │
│                                                                     │
│  transactions.csv                                                   │
│       │                                                             │
│       ▼                                                             │
│  csv_loader.py  ──  normalises columns (aliases → canonical names) │
│       │                                                             │
│       ▼                                                             │
│  main.py  ──  chunks rows into trade groups of 500                  │
│       │                                                             │
│       ├─[1]──► BankingProducer.send_control_doc(TradeDoc)           │
│       │              topic: banking-control                         │
│       │              key:   trade_group_id                          │
│       │                                                             │
│       └─[2]──► BankingProducer.send(row, trade_group_id, trade_id) │
│                      topic: banking-transactions                    │
│                      key:   event_id (UUID)                        │
└─────────────────────────────────────────────────────────────────────┘
                │                        │
                ▼                        ▼
        banking-control          banking-transactions
           (Kafka topic)            (Kafka topic)
                │                        │
                ▼                        ▼
┌─────────────────────────────────────────────────────────────────────┐
│  INGESTION LAYER  (ingestion/)                                      │
│                                                                     │
│  ControlDocListener                  EventBatchListener             │
│  (batch, MANUAL_IMMEDIATE)           (batch, MANUAL_IMMEDIATE)      │
│       │                                      │                      │
│       │  TradeDocMessage                     │  List<ConsumerRecord>│
│       ▼                                      ▼                      │
│  BatchCoordinator.register()         BatchProcessor                 │
│       │                              (8 worker threads)             │
│       │  Creates BatchState:         │                              │
│       │  - trade_group_id            │  Per event:                  │
│       │  - expectedCount             │  1. Parse JSON → EventEnvelope│
│       │  - expectedTradeIds          │  2. Dedup check (Caffeine)   │
│       │  - deadline (now + 5 min)    │  3. Validate required fields │
│       │                              │  4. Check BatchState exists  │
│       │                              │  5. Build ProcessedEvent     │
│       │                              │  6. BatchCoordinator         │
│       │                              │     .recordProcessed()       │
│       │                              │                              │
│       │                     ┌────────┴──────────┐                  │
│       │                     ▼                   ▼                  │
│       │              RetryPublisher        DlqPublisher            │
│       │              (transient errors     (fatal errors,           │
│       │               → banking-           no control doc,          │
│       │               transactions-retry)  retry exhausted          │
│       │                                    → banking-               │
│       │                                    transactions-dlq)        │
│       │                                                             │
│       └──► BatchCoordinator.recordProcessed()                       │
│                   │                                                 │
│                   │  actualCount == expectedCount                   │
│                   │  AND receivedTradeIds == expectedTradeIds?      │
│                   │                                                 │
│                   ├── YES ──► BatchState = SUCCESS                  │
│                   │           StagingBuffer.addAll(events)          │
│                   │                                                 │
│                   └── NO  ──► remain PENDING until deadline         │
│                               Timeout scheduler (every 30s)        │
│                               marks FAIL → DLQ (BATCH_TIMEOUT)     │
│                                                                     │
│  Manual offset commit after every poll (at-least-once delivery)     │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    StagingBuffer
                    (LinkedBlockingQueue<ProcessedEvent>)
                    drain every 5s, up to 1000 events
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│  INTELLIGENCE LAYER  (ingestion/intelligence/)                      │
│                                                                     │
│  IntelligenceProcessor.processBatch(List<ProcessedEvent>)           │
│       │                                                             │
│       │  Per event:                                                 │
│       │                                                             │
│       ├─► SchemaEngine.validate(event)                              │
│       │       Loads: resources/schemas/banking_transaction_v1.json  │
│       │       Checks:                                               │
│       │         - Required fields present (transaction_id,          │
│       │           transaction_date, amount)                         │
│       │         - Type matches schema (string / number / boolean)   │
│       │         - Unknown new fields in payload                     │
│       │       Produces: SchemaValidationResult                      │
│       │         - valid, driftDetected, driftType                   │
│       │         - breakingChange, compatibility, recommendedAction  │
│       │         - violations[]                                      │
│       │                                                             │
│       │       Breaking change? ──YES──► status = REJECTED           │
│       │                                skip quality check           │
│       │                                                             │
│       └─► QualityEngine.check(event)   (only if schema passed)     │
│               Rules (weighted score):                               │
│                 transaction_id.not_null      CRITICAL (weight 4)    │
│                 transaction_date.not_null    CRITICAL (weight 4)    │
│                 amount.not_null              CRITICAL (weight 4)    │
│                 amount.positive              HIGH     (weight 3)    │
│                 transaction_date.iso_format  HIGH     (weight 3)    │
│                 transaction_status.valid     MEDIUM   (weight 2)    │
│                 balance.non_negative         MEDIUM   (weight 2)    │
│                 credit_score.in_range        LOW      (weight 1)    │
│                                                                     │
│               CRITICAL fail → score = 0.0 immediately              │
│               score = passedWeight / totalWeight                    │
│               Produces: QualityCheckResult                          │
│                 - qualityScore, passed, threshold (0.7)             │
│                 - ruleResults[]                                     │
│                                                                     │
│  Status derivation:                                                 │
│    schema breakingChange = true   → REJECTED                        │
│    qualityScore >= 0.7            → PASSED                          │
│    qualityScore >= 0.5            → QUARANTINED                     │
│    qualityScore < 0.5             → REJECTED                        │
│                                                                     │
│  IntelligenceRouter.route(ValidatedEvent)                           │
│       │                                                             │
│       ├── PASSED      ──► MinioStorageWriter → /data/               │
│       ├── QUARANTINED ──► MinioStorageWriter → /quarantine/        │
│       └── REJECTED    ──► DlqPublisher.publishRejected()           │
│                           → banking-transactions-dlq               │
│                           headers: failure_reason, original_topic,  │
│                                    source=intelligence-layer        │
│                                                                     │
│  Batch report logged after every drain cycle:                       │
│    total | passed(%) | quarantined(%) | rejected(%)                 │
│    schema_drift count | avg_quality_score                           │
│    Per-event deviation detail for any non-PASSED events             │
└─────────────────────────────────────────────────────────────────────┘
                    │                   │
              [Phase 2]           [Already live]
                    │                   │
                    ▼                   ▼
             MinIO Storage       banking-transactions-dlq
             /data/              (Kafka topic)
             /quarantine/
                    │
              [Phase 3]
                    │
                    ▼
             Parquet Writer
             (partitioned by date/hour/producer_id)
                    │
              [Phase 4]
                    │
                    ▼
             Query Interface
             (DuckDB / Spark)
                    │
              [Phase 5]
                    │
                    ▼
             REST API
```

---

## Kafka Topics

| Topic | Producer | Consumer | Purpose |
|---|---|---|---|
| `banking-control` | Python producer | `ControlDocListener` | TradeDoc — declares batch before events arrive |
| `banking-transactions` | Python producer | `EventBatchListener` | EventEnvelope — the actual transaction events |
| `banking-transactions-retry` | `RetryPublisher` | `RetryIngestionListener` | Transient failures awaiting re-processing |
| `banking-transactions-dlq` | `DlqPublisher` | — (manual inspection) | Fatal failures, retry-exhausted, intelligence rejects |

---

## Key Data Models

### Producer → Kafka

**TradeDoc** (on `banking-control`)
```
trade_group_id    String        UUID per 500-row chunk
expected_count    int           number of events that will follow
trade_ids         List<String>  exact transaction_id values expected
timestamp         String        ISO-8601 UTC
source            String        "cloud"
pipeline_version  String        "1.0.0"
schema_id         String        "banking_transaction_v1"
```

**EventEnvelope** (on `banking-transactions`)
```
event_id          String        UUID per event
event_version     String        "1.0"
event_type        String        "banking_transaction"
trace_id          String        UUID for lineage
idempotency_key   String        transaction_id (or SHA-256 fallback)
timestamp         String        ISO-8601 UTC
source            String        "cloud"
pipeline_version  String        "1.0.0"
schema_id         String        "banking_transaction_v1"
trade_group_id    String        links event to its TradeDoc
trade_id          String        transaction_id from CSV
payload           Map           normalised CSV row fields
```

### Ingestion → StagingBuffer

**ProcessedEvent**
```
eventId           String
tradeGroupId      String
tradeId           String
idempotencyKey    String
eventType         String
timestamp         String
schemaId          String
source            String
pipelineVersion   String
payload           Map<String, Object>
```

### Intelligence Layer → Storage (Phase 2)

**ValidatedEvent**
```
event             ProcessedEvent
schemaResult      SchemaValidationResult
  ├── schemaId, schemaVersion, valid
  ├── driftDetected, driftType
  ├── breakingChange, compatibility
  ├── recommendedAction
  └── violations[]  (field, expectedType, actualType, violationType)
qualityResult     QualityCheckResult
  ├── qualityScore (0.0 – 1.0)
  ├── passed, threshold
  └── ruleResults[] (ruleId, field, severity, passed, actualValue, message)
validationStatus  PASSED | QUARANTINED | REJECTED
ingestionTime     String   ISO-8601 UTC (stamped by intelligence layer)
pipelineVersion   String
sourceTopic       String   "banking-transactions"
```

---

## Error Routing Summary

| Error | Where caught | Destination |
|---|---|---|
| JSON parse failure | `BatchProcessor` | DLQ — `FATAL_PARSE_ERROR` |
| Missing `trade_group_id` or `trade_id` | `BatchProcessor` | DLQ — `FATAL_MISSING_FIELDS` |
| No registered TradeDoc for trade group | `BatchProcessor` | DLQ — `NO_CONTROL_DOC` |
| Transient processing error | `BatchProcessor` | Retry topic (max 3 attempts, 2s→4s→8s backoff) |
| Retry exhausted | `RetryPublisher` | DLQ — `RETRY_EXHAUSTED` |
| Batch timeout (5 min) | `BatchCoordinator` scheduler | DLQ — `BATCH_TIMEOUT` |
| Breaking schema change | `IntelligenceProcessor` | DLQ — `SCHEMA_BREAKING_CHANGE:<type>` |
| Quality score < 0.5 or CRITICAL rule fail | `IntelligenceProcessor` | DLQ — `QUALITY_REJECTED:score=<n>` |
| Quality score 0.5–0.69 | `IntelligenceProcessor` | MinIO `/quarantine/` (Phase 2) |

---

## Infrastructure (Docker Compose)

| Service | Image | Port | Purpose |
|---|---|---|---|
| `zookeeper` | confluentinc/cp-zookeeper:7.6.0 | 2181 | Kafka coordination |
| `kafka` | confluentinc/cp-kafka:7.6.0 | 9092 | Message broker |
| `kafka-ui` | provectuslabs/kafka-ui | 8081 | Topic browser / message inspector |
| `minio` | minio/minio:RELEASE.2024-03-30T09-41-56Z | 9000 (S3 API), 9001 (Console UI) | S3-compatible object storage |

---

## Running the Pipeline

```bash
# 1. Start Kafka infrastructure
cd producers
docker-compose up -d

# 2. Start ingestion + intelligence app
cd ingestion
mvn spring-boot:run

# 3. Run the producer
cd producers/cloud
python -m banking_producer
```

---

## Phase 3 — What Changes Next

Parquet writer — converts JSON events in MinIO to partitioned Parquet files.
Target path: `adaptive-data-lake/parquet/year=YYYY/month=MM/day=DD/hour=HH/source=S/part-N.parquet`

`MinioStorageWriter`, `IntelligenceRouter`, and all upstream components require zero changes.
