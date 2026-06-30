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

---

## Phase 4 — Intelligence Layer

| # | Task | Status | Notes |
|---|---|---|---|
| 4.1 | Define validation engine output contract | ✅ Done | `SchemaValidationResult`, `QualityCheckResult`, `ValidatedEvent` |
| 4.2 | All model classes + enums | ✅ Done | `DriftType`, `Compatibility`, `RecommendedAction`, `Severity`, `ValidationStatus` |
| 4.3 | Flat-file schema registry | ✅ Done | `resources/schemas/banking_transaction_v1.json` |
| 4.4 | `SchemaEngine` — loads schemas at startup, validates + detects drift | ✅ Done | Detects NEW_FIELD, MISSING_FIELD, TYPE_CHANGE; derives breaking change + compatibility |
| 4.5 | `QualityRule` interface | ✅ Done | `ruleId()` + `evaluate(payload)` |
| 4.6 | Quality rules (6 rules, 4 severity levels) | ✅ Done | `NotNullRule` (CRITICAL ×3), `PositiveAmountRule` (HIGH), `DateFormatRule` (HIGH), `ValidStatusRule` (MEDIUM), `NonNegativeBalanceRule` (MEDIUM), `CreditScoreRangeRule` (LOW) |
| 4.7 | `QualityEngine` — weighted scoring, CRITICAL short-circuit | ✅ Done | Threshold 0.7 (configurable) |
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
| 6.1 | Add Apache Parquet + Hadoop dependencies to `pom.xml` | 🔲 Pending | |
| 6.2 | `ParquetWriter` — converts JSON events to Parquet schema | 🔲 Pending | Define Parquet schema matching `ValidatedEvent` |
| 6.3 | Partitioned writes to MinIO (`parquet/year=/month=/day=/hour=/source=`) | 🔲 Pending | Reuse same partition pattern as JSON writer |
| 6.4 | File size target ~128MB (batch/compact before writing) | 🔲 Pending | Accumulate events until size threshold or time window |
| 6.5 | Atomic partition publish (write temp → rename) | 🔲 Pending | Prevents partial reads during write |
| 6.6 | Small-file compaction strategy | ⏸ Deferred | Needed for production; not required for Phase 6 baseline |

---

## Phase 7 — Query Interface

| # | Task | Status | Notes |
|---|---|---|---|
| 7.1 | DuckDB adapter — queries Parquet files directly from MinIO | 🔲 Pending | Python service or embedded Java |
| 7.2 | Spark adapter — distributed queries over large datasets | 🔲 Pending | Separate service |
| 7.3 | SQL dialect portability layer | 🔲 Pending | Common function subset between DuckDB and Spark |
| 7.4 | Predicate pushdown + column pruning on Parquet partitions | 🔲 Pending | Use partition keys in WHERE clauses |

---

## Phase 8 — REST API

| # | Task | Status | Notes |
|---|---|---|---|
| 8.1 | Spring Boot REST controller | 🔲 Pending | |
| 8.2 | Engine routing (DuckDB vs Spark) based on query size/type | 🔲 Pending | |
| 8.3 | Pagination and sorting | 🔲 Pending | |
| 8.4 | Response schema standards | 🔲 Pending | |
| 8.5 | Timeout behavior for Spark-backed queries | 🔲 Pending | |

---

## Phase 9 — Observability

| # | Task | Status | Notes |
|---|---|---|---|
| 9.1 | Consumer lag dashboard (Kafka) | 🔲 Pending | |
| 9.2 | Ingestion throughput metrics | 🔲 Pending | |
| 9.3 | Quality score trend charts | 🔲 Pending | |
| 9.4 | Schema drift alerts | 🔲 Pending | |
| 9.5 | Query latency p95 | 🔲 Pending | |

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
| `flow.md` | Full pipeline flow diagram, models, error routing, run instructions | ✅ Up to date |
| `architecture.md` | PRD review, alignment check, module contracts, prioritised steps | ✅ Reference |
| `ingestion/DESIGN.md` | Ingestion layer internals, class design, batch validation logic | ✅ Reference |
| `TASKS.md` | This file — task tracker | ✅ Up to date |
