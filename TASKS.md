# Adaptive Data Lake — Task Tracker

> Update this file whenever a task status changes.
> Statuses: ✅ Done | 🔲 Pending | 🚧 In Progress | ⏸ Deferred

---

## Phase 1 — Producer

| # | Task | Status | Notes |
|---|---|---|---|
| 1.1 | Python producer reads CSV and streams rows | ✅ Done | `csv_loader.py` with column alias normalisation |
| 1.2 | EventEnvelope schema (Pydantic) | ✅ Done | `schema.py` — includes `trade_group_id`, `trade_id`, `idempotency_key` |
| 1.3 | TradeDoc schema (Pydantic) | ✅ Done | `schema.py` — control document sent before events |
| 1.4 | Producer sends TradeDoc to `banking-control` first | ✅ Done | Fixed — was missing, caused all events to DLQ |
| 1.5 | Producer sends events with `trade_group_id` + `trade_id` | ✅ Done | Fixed — fields were null, caused `FATAL_MISSING_FIELDS` |
| 1.6 | Chunk CSV into trade groups of 500 | ✅ Done | Prevents TradeDoc exceeding Kafka 1MB message limit |
| 1.7 | `__main__.py` entry point for `python -m banking_producer` | ✅ Done | |
| 1.8 | Producer generalised to any per-row-ID dataset via config | ✅ Done | `ID_FIELD`, `EVENT_TYPE` added alongside existing `SCHEMA_ID` in `config.py` — trade_id/idempotency-key field name no longer hardcoded to `transaction_id`. Enabled TPC-H onboarding with no new producer code; see `tpch_ingestion.md` §4. |

---

## Phase 2 — Kafka Infrastructure

| # | Task | Status | Notes |
|---|---|---|---|
| 2.1 | Docker Compose with Zookeeper + Kafka | ✅ Done | Confluent Platform 7.6.0 |
| 2.2 | Kafka UI for topic browsing | ✅ Done | `http://localhost:8081` |
| 2.3 | Topics auto-created by Kafka | ✅ Done | `KAFKA_AUTO_CREATE_TOPICS_ENABLE: true` |
| 2.4 | MinIO S3-compatible storage | ✅ Done | `http://localhost:9000` API, `http://localhost:9001` Console |

---

## Phase 3 — Ingestion Layer (Spring Boot)

| # | Task | Status | Notes |
|---|---|---|---|
| 3.1 | `ControlDocListener` — consumes `banking-control` | ✅ Done | Batch, MANUAL_IMMEDIATE ack |
| 3.2 | `EventBatchListener` — consumes `banking-transactions` | ✅ Done | Batch, MANUAL_IMMEDIATE ack |
| 3.3 | `BatchCoordinator` — registers TradeDoc, tracks completeness | ✅ Done | Checks count AND trade_id set equality |
| 3.4 | `BatchProcessor` — 8-worker pool, per-event pipeline | ✅ Done | Dedup → validate → process → route |
| 3.5 | `DeduplicationStore` — Caffeine cache on `idempotency_key` | ✅ Done | 60-min TTL |
| 3.6 | `RetryPublisher` — exponential backoff headers | ✅ Done | 2s → 4s → 8s, max 3 attempts |
| 3.7 | `DlqPublisher` — fatal errors + batch timeout + intelligence rejects | ✅ Done | Includes `publishRejected()` for intelligence layer |
| 3.8 | `RetryIngestionListener` — re-processes retry topic events | ✅ Done | Reads `retry_after` header before processing |
| 3.9 | `StagingBuffer` — `LinkedBlockingQueue`, drains every 5s | ✅ Done | Wired to `IntelligenceProcessor` |
| 3.10 | Timeout scheduler — marks FAIL batches after 5 min | ✅ Done | Runs every 30s |
| 3.11 | Manual offset commit (at-least-once) | ✅ Done | Committed after all events in poll resolved |
| 3.12 | `NO_CONTROL_DOC` treated as transient, not fatal | ✅ Done | `BatchProcessor` now retries (2s→4s→8s, max 3 attempts) before DLQ, instead of DLQ on first miss — fixes a race between `ControlDocListener` and `EventBatchListener` polling independently that was losing up to ~55% of events on multi-batch producer runs. See `tpch_ingestion.md` §5. |

### Phase 3 Tests — all passing ✅

| Test class | Tests | Covers |
|---|---|---|
| `IngestionFlowIntegrationTest` | 3/3 | Happy path (3 events → staged), deduplication (5 unique + 2 dups → exactly 5 staged), no control doc → not staged |
| `BatchCoordinatorTest` | 7/7 | Register, all events → staging, partial → pending, timeout at 0 min, completed batch survives timeout check |
| `DeduplicationStoreTest` | 5/5 | Unseen=false, marked=true, independent keys, double-mark idempotent, multiple keys |

---

## Phase 4 — Intelligence Layer

| # | Task | Status | Notes |
|---|---|---|---|
| 4.1 | Define validation engine output contract | ✅ Done | `SchemaValidationResult`, `QualityCheckResult`, `ValidatedEvent` |
| 4.2 | All model classes + enums | ✅ Done | `DriftType`, `Compatibility`, `RecommendedAction`, `Severity`, `ValidationStatus` |
| 4.3 | Flat-file schema registry | ✅ Done | `resources/schemas/*.json`, auto-loaded by `schema_id` — now includes `banking_transaction_v1`, `tpch_orders_v1`, `tpch_lineitem_v1` (see `tpch_ingestion.md`) |
| 4.4 | `SchemaEngine` — loads schemas at startup, validates + detects drift | ✅ Done | Detects NEW_FIELD, MISSING_FIELD, TYPE_CHANGE; derives breaking change + compatibility |
| 4.5 | `QualityRule` interface | ✅ Done | `ruleId()` + `appliesTo(schemaId)` + `evaluate(payload)` — `appliesTo` added so rules from one dataset never score another dataset's events |
| 4.6 | Quality rules, scoped per schema_id | ✅ Done | Banking (`banking_transaction_v1`): `NotNullRule` (CRITICAL ×3), `PositiveNumberRule` (HIGH, formerly `PositiveAmountRule`), `DateFormatRule` (HIGH), `ValidStatusRule` (MEDIUM), `NonNegativeBalanceRule` (MEDIUM), `CreditScoreRangeRule` (LOW). TPC-H rules added per `tpch_ingestion.md` §3. |
| 4.7 | `QualityEngine` — weighted scoring, CRITICAL short-circuit | ✅ Done | Threshold 0.7 (configurable); filters to rules matching the event's `schema_id` before scoring |
| 4.8 | `IntelligenceProcessor` — orchestrates engines, batch report | ✅ Done | Logs per-event deviation detail |
| 4.9 | `IntelligenceRouter` — routes PASSED/QUARANTINED/REJECTED | ✅ Done | Writes to MinIO or DLQ |

---

## Phase 5 — MinIO Storage Writer

| # | Task | Status | Notes |
|---|---|---|---|
| 5.1 | `MinioProperties` + `MinioConfig` Spring beans | ✅ Done | `S3Client` with `pathStyleAccessEnabled(true)` for MinIO |
| 5.2 | Bucket auto-creation on startup | ✅ Done | `headBucket` check → `createBucket` if absent |
| 5.3 | `MinioStorageWriter` — writes `ValidatedEvent` as JSON | ✅ Done | Hive-style partition keys: `year=/month=/day=/hour=/source=` |
| 5.4 | PASSED events → `data/` prefix | ✅ Done | |
| 5.5 | QUARANTINED events → `quarantine/` prefix | ✅ Done | |
| 5.6 | REJECTED events → DLQ (`banking-transactions-dlq`) | ✅ Done | |

---

## Phase 6 — Parquet Writer

| # | Task | Status | Notes |
|---|---|---|---|
| 6.1 | Add Apache Parquet + Hadoop dependencies to `pom.xml` | ✅ Done | `parquet-avro 1.13.1`, `avro 1.11.3`, `hadoop-client-api/runtime 3.3.6` |
| 6.2 | Avro schema (`banking_transaction_record.avsc`) + `ParquetRecordMapper` | ✅ Done | Flattened 25-column schema; nested violations/rule results JSON-encoded as strings |
| 6.3 | `ParquetStorageWriter` — writes batch as temp file then `PutObject` to MinIO | ✅ Done | Snappy compression + dictionary encoding; temp file deleted after upload |
| 6.4 | `ParquetWriteBuffer` — buffers events per partition, flushes by count or time | ✅ Done | Default 10 000 events or 60 s, both configurable in `application.yml` |
| 6.5 | Atomic partition publish (write local temp → single `PutObject`) | ✅ Done | Object only appears in bucket once fully written; no partial-file reads possible |
| 6.6 | Wire `IntelligenceRouter` to `ParquetWriteBuffer`; remove `MinioStorageWriter` | ✅ Done | PASSED → `data/`, QUARANTINED → `quarantine/`; JSON writer deleted |
| 6.7 | Graceful shutdown flush (`@PreDestroy`) | ✅ Done | Remaining buffered events flushed to Parquet before app stops |
| 6.8 | Small-file compaction strategy | ⏸ Deferred | Needed for production; avoid with wide flush windows for now |

---

## Phase 7 — Query Interface

| # | Task | Status | Notes |
|---|---|---|---|
| 7.1 | `QueryEngine` interface + `QueryFilter` / `QueryResult` models | ✅ Done | Common contract; engine-agnostic |
| 7.2 | `PartitionPathBuilder` — S3 glob builder with Hive partition pruning | ✅ Done | Builds `s3://bucket/prefix/year=.../.../**/*.parquet` progressively |
| 7.3 | `DuckDbQueryEngine` — full JDBC implementation | ✅ Done | `duckdb_jdbc 0.10.3`; httpfs extension; configures MinIO S3 endpoint at startup |
| 7.4 | `SparkQueryEngine` — compilable scaffold | ✅ Done | `@ConditionalOnProperty(spark.enabled=true)`; throws `UnsupportedOperationException` until `spark-sql_2.12` dep added |
| 7.5 | `QueryEngineRouter` — routes DuckDB vs Spark per `QueryFilter.preferSpark` | ✅ Done | Spark injected as `Optional<>` — absent when disabled |
| 7.6 | Predicate pushdown — partition path + DuckDB `hive_partitioning=true` | ✅ Done | Directory-level pruning in S3 + row-group pruning inside each file |
| 7.7 | Full Spark implementation | ⏸ Deferred | Add `spark-sql_2.12:3.5.x` + `hadoop-aws:3.3.6`, set `spark.enabled=true`, implement `SparkQueryEngine.execute()` |

---

## Phase 8 — REST API

| # | Task | Status | Notes |
|---|---|---|---|
| 8.1 | `QueryController` — GET + POST `/api/v1/query` | ✅ Done | `spring-boot-starter-web`; delegates to `QueryEngineRouter` |
| 8.2 | Engine routing (DuckDB vs Spark) via `preferSpark` flag | ✅ Done | `QueryEngineRouter` with `Optional<SparkQueryEngine>` |
| 8.3 | Pagination — `page` + `pageSize` params, `totalPages` in response | ✅ Done | `QueryFilter.page/pageSize`; `DuckDbQueryEngine` runs `COUNT(*)` + `LIMIT/OFFSET`; `QueryResult` adds `page`, `pageSize`, `totalPages`, `returnedRows` (`totalRows` now = total matches across all pages) |
| 8.4 | Standardised error response body (`ApiError` with `code`, `message`, `path`) | ✅ Done | New `web` package: `ApiError` record (`code`, `message`, `path`, `timestamp`, `details`) + `GlobalExceptionHandler` (`@RestControllerAdvice`) covering validation, malformed JSON, type mismatch, query execution, and unhandled errors |
| 8.5 | Request validation (`@Valid`, `@Min`/`@Max` on filter params) | ✅ Done | `spring-boot-starter-validation` added; `QueryFilter` fields constrained (year 2000–2100, month 1–12, day 1–31, hour 0–23, maxRows ≤100k, pageSize ≤10k); POST body validated via `@Valid`, GET params via `@Validated` |
| 8.6 | Timeout behavior for Spark-backed queries | ⏸ Deferred | Deferred until `SparkQueryEngine` is fully implemented |

### Phase 8 Tests — all passing ✅

| Test class | Tests | Covers |
|---|---|---|
| `QueryControllerTest` | 12/12 | GET/POST happy path, GET param → `QueryFilter` wiring, defaults, pagination fields in response, validation failures (month/year/page/pageSize out of range, non-numeric year), malformed JSON body, `QueryExecutionException` → 500 `ApiError` |

Full suite: 27/27 passing (`mvn test`).

---

## Phase 9 — Observability

| # | Task | Status | Notes |
|---|---|---|---|
| 9.1 | Add `spring-boot-starter-actuator` + `micrometer-registry-prometheus` | 🔲 Pending | Exposes `/actuator/prometheus` scrape endpoint |
| 9.2 | Custom counter — events ingested / DLQ'd / retried per topic | 🔲 Pending | `MeterRegistry` injected into `BatchProcessor` + `DlqPublisher` |
| 9.3 | Custom gauge — `StagingBuffer` queue depth | 🔲 Pending | `Gauge.builder("staging.buffer.size", ...)` |
| 9.4 | Custom timer — batch processing latency (control doc received → staged) | 🔲 Pending | `Timer` in `BatchCoordinator` |
| 9.5 | Custom counter — quality score distribution (PASSED / QUARANTINED / REJECTED) | 🔲 Pending | Tag by `validation_status` in `IntelligenceProcessor` |
| 9.6 | Custom counter — schema drift events by drift type | 🔲 Pending | Tag by `drift_type` in `SchemaEngine` |
| 9.7 | Custom timer — DuckDB query execution latency | 🔲 Pending | Wrap `DuckDbQueryEngine.execute()` |
| 9.8 | Prometheus + Grafana in Docker Compose | 🔲 Pending | Scrapes `:8080/actuator/prometheus`; prebuilt dashboards |

---

## Phase 10 — Hudi Integration

| # | Task | Status | Notes |
|---|---|---|---|
| 10.1 | Add Hudi behind feature flag | ⏸ Deferred | After baseline Parquet path is stable |
| 10.2 | Upsert / merge-on-read strategy | ⏸ Deferred | |

---

## Deferred / Out of Scope (Current Iteration)

| Task | Reason |
|---|---|
| Cloud ingestion (Dataflow → GCS) | Out of scope — on-prem only for current iteration |
| Backpressure plan (consumer lag thresholds, autoscaling) | Operations phase |
| SLOs (ingestion latency p95, batch completion time p95, error budget) | Operations phase |
| IAM / RBAC matrix for Kafka and MinIO | Operations phase |
| Encryption at rest / in transit | Operations phase |
| PII tagging / redaction in intelligence layer | Future iteration |
| Audit logs for schema changes and quality overrides | Future iteration |
| Data retention + lifecycle rules | Future iteration |
| Disaster recovery (RPO / RTO definitions) | Future iteration |
| External schema registry (Confluent Schema Registry) | Phase 4 used flat files; registry is next evolution |
| Externalised quality rules (YAML rulesets) | Currently Java code; YAML externalisation is next evolution |
| Separate Python intelligence service | Currently Java (same app); Python split is next evolution |

---

## Documentation

| File | Purpose | Status |
|---|---|---|
| `flow.md` | Full pipeline flow diagram, models, error routing, run instructions | ✅ Up to date through Phase 8 |
| `architecture.md` | PRD review, alignment check, module contracts, prioritised steps | ✅ Reference |
| `ingestion/DESIGN.md` | Ingestion layer internals, class design, batch validation logic | ✅ Reference |
| `tpch_ingestion.md` | TPC-H dataset onboarding — data generation, schema, quality rules, control doc, Parquet rollover sizing | ✅ Reference |
| `TASKS.md` | This file — task tracker | ✅ Up to date |
