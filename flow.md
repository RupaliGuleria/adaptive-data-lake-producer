# Adaptive Data Lake — Pipeline Flow

> Living document. Updated as each layer is built.
> For architectural decisions and trade-offs see `architecture.md`.
> For ingestion internals see `ingestion/DESIGN.md`.
> For TPC-H dataset onboarding (data generation, schema, quality rules) see `tpch_ingestion.md`.

---

## Current Build Status

| Layer | Status | Technology |
|---|---|---|
| Producer | ✅ Built | Python, confluent-kafka, Pydantic |
| Kafka Infrastructure | ✅ Built | Docker Compose, Confluent Platform 7.6 |
| Ingestion (on-prem) | ✅ Built | Java 21, Spring Boot 3.2, Spring Kafka |
| Intelligence Layer | ✅ Built | Java 21, Spring Boot (same app) |
| Storage — MinIO | ✅ Built | MinIO (S3-compatible), AWS SDK v2, Docker Compose |
| Parquet Writer | ✅ Built | Apache Parquet + Avro, Snappy, buffered → MinIO |
| Query Interface | ✅ Built | DuckDB JDBC (in-process); Spark scaffold ready |
| REST API | ✅ Built | Spring Boot — `QueryController`, pagination, validation, standardised errors |

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
│       │              (transient errors,     (fatal errors,          │
│       │               no control doc yet    retry exhausted         │
│       │               → banking-            → banking-              │
│       │               transactions-retry)   transactions-dlq)       │
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
│       │       Loads: all files under resources/schemas/*.json,     │
│       │       registered by schema_id (banking_transaction_v1,      │
│       │       tpch_orders_v1, tpch_lineitem_v1 — see                │
│       │       tpch_ingestion.md)                                    │
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
│               Each QualityRule bean declares which schema_id it     │
│               applies to; only rules matching the event's schema    │
│               are evaluated (see tpch_ingestion.md for the          │
│               TPC-H rule set). Rules below apply to                 │
│               banking_transaction_v1 (weighted score):              │
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
│       ├── PASSED      ──► ParquetWriteBuffer.add("data", event)    │
│       ├── QUARANTINED ──► ParquetWriteBuffer.add("quarantine", event)│
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
                    ▼                   ▼
          ParquetWriteBuffer    banking-transactions-dlq
          (in-memory, per        (Kafka topic)
           partition bucket)
                    │ flush on 10 000 events OR 60 s
                    ▼
          ParquetStorageWriter
          1. write local temp file (Snappy + dict encoding)
          2. PutObject → MinIO  ← atomic: object appears only when complete
          3. delete temp file
                    │
                    ▼
             MinIO Parquet
             data/year=YYYY/month=MM/day=DD/hour=HH/source=S/part-N-T.parquet
             quarantine/year=…/…/part-N-T.parquet
                    │
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│  QUERY INTERFACE  (ingestion/query/)                                │
│                                                                     │
│  QueryEngineRouter.execute(QueryFilter)                             │
│       │                                                             │
│       ├── preferSpark=false (default)                               │
│       │       └──► DuckDbQueryEngine                                │
│       │              @PostConstruct: LOAD httpfs, SET s3_endpoint,  │
│       │                             s3_url_style=path, no SSL       │
│       │              Builds: read_parquet('s3://bucket/prefix/      │
│       │                        year=.../month=../**/*.parquet',     │
│       │                        hive_partitioning=true)              │
│       │              WHERE <partition + additional predicates>       │
│       │              LIMIT <maxRows>                                 │
│       │              Returns: QueryResult (columns, rows, timing)   │
│       │                                                             │
│       └── preferSpark=true (when spark.enabled=true)               │
│               └──► SparkQueryEngine  ← scaffold only; activate by  │
│                                        adding spark-sql_2.12 dep   │
│                                                                     │
│  PartitionPathBuilder pushes known partitions into the S3 path     │
│  (directory pruning) then residual filters become WHERE clauses    │
│  (row-group pruning inside Parquet files).                          │
│                                                                     │
│  Pagination: page/pageSize → OFFSET/LIMIT; a COUNT(*) query runs   │
│  alongside the paged SELECT to populate totalRows/totalPages.      │
└─────────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│  REST API  (ingestion/query/, ingestion/web/)                       │
│                                                                     │
│  QueryController                                                    │
│    GET  /api/v1/query   — query params (prefix, year..hour, source,│
│                            where, maxRows, page, pageSize,           │
│                            preferSpark) — browser/curl friendly     │
│    POST /api/v1/query   — QueryFilter as JSON body                  │
│       │                                                             │
│       │  @Validated (GET params) / @Valid (POST body)               │
│       │  year 2000–2100, month 1–12, day 1–31, hour 0–23,           │
│       │  maxRows ≤ 100 000, pageSize ≤ 10 000                       │
│       ▼                                                             │
│  QueryEngineRouter.execute(filter)  ──►  QueryResult                │
│       │                                                             │
│       │  on any failure ▼                                           │
│  GlobalExceptionHandler (@RestControllerAdvice)                     │
│    validation / malformed JSON / type mismatch → 400 ApiError       │
│    QueryExecutionException                      → 500 ApiError      │
│    anything unhandled                           → 500 ApiError      │
│  ApiError { code, message, path, timestamp, details[] }             │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Kafka Topics

| Topic | Producer | Consumer | Purpose |
|---|---|---|---|
| `banking-control` | Python producer | `ControlDocListener` | TradeDoc — declares batch before events arrive |
| `banking-transactions` | Python producer | `EventBatchListener` | EventEnvelope — the actual transaction events |
| `banking-transactions-retry` | `RetryPublisher` | `RetryIngestionListener` | Transient failures awaiting re-processing |
| `banking-transactions-dlq` | `DlqPublisher` | — (manual inspection) | Fatal failures, retry-exhausted, intelligence rejects |

Despite the topic names, these four topics are shared by every onboarded dataset, not
banking-exclusive — each event's `schema_id` field (in its `EventEnvelope`/`TradeDoc`) is
what distinguishes banking rows from TPC-H rows on the same topic. See `tpch_ingestion.md`.

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

### Intelligence Layer → Parquet Storage (Phase 6)

**BankingTransactionRecord** (Avro schema — `resources/avro/banking_transaction_record.avsc`)
```
event_id                String
trade_group_id          String
trade_id                String
idempotency_key         String
event_type              String
timestamp               String        ISO-8601 UTC
source                  String        low-cardinality → dictionary-encoded
pipeline_version        String
schema_id               String
payload_json            String        normalised CSV row, JSON-serialised

validation_status       String        PASSED | QUARANTINED | REJECTED
schema_version          String
schema_valid            Boolean
drift_detected          Boolean
drift_type              String?       NEW_FIELD | MISSING_FIELD | TYPE_CHANGE | FORMAT_CHANGE
breaking_change         Boolean
compatibility           String?       BACKWARD | FORWARD | FULL | NONE
schema_violations_json  String        List<SchemaViolation>, JSON-serialised

quality_score           Double        0.0 – 1.0
quality_passed          Boolean
quality_threshold       Double        0.7 default
quality_rule_results_json String      List<RuleResult>, JSON-serialised

ingestion_time          String        ISO-8601 UTC
source_topic            String        "banking-transactions"
```

Compression: **Snappy** | Dictionary encoding: **on** | Partition path: Hive-style  
`{prefix}/year=YYYY/month=MM/day=DD/hour=HH/source=S/part-N-<epoch>.parquet`

### Intelligence Layer → ValidatedEvent model

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

### Query Layer → REST API

**QueryFilter** (request — GET params or POST body)
```
prefix            String   "data" | "quarantine"                default "data"
year/month/day/hour  Integer  Hive partition columns, progressively narrowing
source            String   optional partition column
additionalWhere   String   extra SQL predicate, appended as-is
maxRows           int      safety cap                            default 10 000, ≤100 000
page              int      1-based page number                   default 1
pageSize          int      rows per page                         default 100, ≤10 000
preferSpark       boolean  route to Spark instead of DuckDB       default false
```

**QueryResult** (response)
```
columns           List<String>
rows              List<Map<String,Object>>   this page's rows
totalRows         int      total matches across all pages (from COUNT(*))
returnedRows      int      rows.size() for this page
page / pageSize / totalPages  int
executionTimeMs   long
engine            String   "duckdb" | "spark"
scannedPath       String   S3 glob that was scanned
```

**ApiError** (error response, any 4xx/5xx)
```
code              String   VALIDATION_ERROR | MISSING_PARAMETER | TYPE_MISMATCH |
                           MALFORMED_REQUEST | QUERY_EXECUTION_ERROR | INTERNAL_ERROR
message           String
path              String   request URI
timestamp         Instant
details           List<String>   e.g. per-field validation messages
```

---

## Error Routing Summary

| Error | Where caught | Destination |
|---|---|---|
| JSON parse failure | `BatchProcessor` | DLQ — `FATAL_PARSE_ERROR` |
| Missing `trade_group_id` or `trade_id` | `BatchProcessor` | DLQ — `FATAL_MISSING_FIELDS` |
| No registered TradeDoc for trade group (yet) | `BatchProcessor` | Retry topic (max 3 attempts, 2s→4s→8s backoff) → DLQ `NO_CONTROL_DOC` only if still unregistered after retries |
| Transient processing error | `BatchProcessor` | Retry topic (max 3 attempts, 2s→4s→8s backoff) |
| Retry exhausted | `RetryPublisher` | DLQ — `RETRY_EXHAUSTED` |
| Batch timeout (5 min) | `BatchCoordinator` scheduler | DLQ — `BATCH_TIMEOUT` |
| Breaking schema change | `IntelligenceProcessor` | DLQ — `SCHEMA_BREAKING_CHANGE:<type>` |
| Quality score < 0.5 or CRITICAL rule fail | `IntelligenceProcessor` | DLQ — `QUALITY_REJECTED:score=<n>` |
| Quality score 0.5–0.69 | `IntelligenceProcessor` | Parquet buffer → MinIO `quarantine/` |

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

## Phase 8 — REST API (Built)

`QueryController` exposes `QueryEngineRouter` over HTTP:
```
GET  /api/v1/query   — query params (prefix, year..hour, source, where, maxRows, page, pageSize, preferSpark)
POST /api/v1/query   — QueryFilter as JSON body
```

Pagination (`page`/`pageSize` → `totalPages`), request validation (`@Valid`/`@Validated`), and standardised
`ApiError` responses (via `GlobalExceptionHandler`) are all in place — see "Query Layer → REST API" models above.

Not yet built: `GET /api/v1/query/partitions` (list available partition combinations), and timeout handling
for Spark-backed queries (deferred until `SparkQueryEngine` is fully implemented).

## Phase 9 — What Changes Next

**Observability** — Actuator + Micrometer/Prometheus, custom counters/gauges/timers across ingestion,
intelligence, and query layers, plus Grafana dashboards. See `TASKS.md` Phase 9 for the full task list.
