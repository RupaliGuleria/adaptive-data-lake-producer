# Ingestion Layer — On-Prem Design

## Scope

On-prem ingestion only. Reads banking transaction events from Kafka, processes
them in parallel using an internal worker pool, and writes clean events to an
in-memory staging buffer (Phase 1). Storage target after staging is
S3-compatible (S3/StorageGrid).

Cloud ingestion (Dataflow → GCS path) is out of scope for this iteration.

---

## Decided Constraints

| Concern | Decision |
|---|---|
| Framework | Spring Boot + Spring Kafka |
| Offset commit | Manual, at-least-once |
| Starting partitions | 1 (scale with benchmarking) |
| Worker threads per batch | Configurable, default 8 |
| Batch poll size | 500 events (`max.poll.records`) |
| Staging target (Phase 1) | In-memory `LinkedBlockingQueue` |
| Staging target (Phase 2) | MinIO (S3-compatible, Docker Compose) |
| Max retry attempts | 3 with exponential backoff (2s → 4s → 8s) |
| Transient error path | Retry topic → DLQ on exhaustion |
| Fatal error path | Straight to DLQ, no retries |
| Batch completion timeout | 5 minutes (configurable) |

### Timeout Rationale

5 minutes is chosen because the producer flushes every 500 events and a trade
group may span multiple producer flush cycles. 5 minutes survives minor network
hiccups and slow producer throughput while surfacing genuine failures quickly
enough for local testing. Tune upward for production-scale trade groups via
`ingestion.batch.timeout-minutes`.

---

## Topic Layout

| Topic | Purpose |
|---|---|
| `banking-control` | Control documents (`TradeDoc`) from producer |
| `banking-transactions` | Event envelopes (`EventEnvelope`) from producer |
| `banking-transactions-retry` | Transient failures awaiting re-processing |
| `banking-transactions-dlq` | Fatal failures and retry-exhausted events |

---

## Control Doc Flow

The producer sends a `TradeDoc` to `banking-control` **before** publishing the
corresponding event batch to `banking-transactions`. The `TradeDoc` declares:

- `trade_group_id` — the logical batch identifier
- `expected_count` — how many events will arrive for this group
- `trade_ids` — the exact set of trade IDs expected

The ingestion layer uses this to validate completeness after deduplication.

```
Producer
  │
  ├─[1]──► banking-control      TradeDoc { trade_group_id, expected_count, trade_ids }
  │
  └─[2]──► banking-transactions EventEnvelope { trade_group_id, trade_id, idempotency_key, ... }
                                 (500 events per flush, N flushes per trade group)
```

---

## Core Classes

### BatchConsumer

Owns two Kafka listeners:

- **ControlDocListener** — batch listener on `banking-control`. Parses each
  `TradeDoc` and calls `BatchCoordinator.register(tradeDoc)`.
- **EventBatchListener** — batch listener on `banking-transactions`. Hands the
  polled `List<ConsumerRecord>` to `BatchProcessor` for processing. Commits
  offset only after `BatchProcessor` returns (all events in the poll resolved).

Both listeners use `AckMode.MANUAL_IMMEDIATE`.

---

### BatchCoordinator

Central state manager for all in-flight trade group batches. Thread-safe.

**State per trade group** (`BatchState`):

| Field | Type | Description |
|---|---|---|
| `tradeGroupId` | `String` | Partition key |
| `expectedCount` | `int` | From `TradeDoc` |
| `expectedTradeIds` | `Set<String>` | From `TradeDoc.trade_ids` |
| `receivedTradeIds` | `ConcurrentHashMap.newKeySet()` | Accumulated after dedup |
| `actualCount` | `AtomicInteger` | Incremented by `BatchProcessor` |
| `status` | `BatchStatus` | `PENDING / SUCCESS / FAIL` |
| `deadline` | `Instant` | `registeredAt + 5 min` |

**Key methods:**

```
register(TradeDoc)
  → Creates BatchState with status=PENDING and deadline=now+timeout
  → If trade_group_id already exists and is PENDING → warn and overwrite

recordProcessed(tradeGroupId, tradeId)
  → Adds tradeId to receivedTradeIds (idempotent — Set ignores duplicates)
  → Increments actualCount
  → If actualCount == expectedCount AND receivedTradeIds == expectedTradeIds
        → status = SUCCESS, drain BatchState to StagingBuffer

checkTimeouts()   [called by scheduler every 30 seconds]
  → For each PENDING BatchState where now > deadline
        → status = FAIL
        → log discrepancy: expected N, received M, missing trade_ids
        → publish BatchFailEvent to DLQ
```

**No control doc for an event:**

If `EventBatchListener` receives an event whose `trade_group_id` has no
registered `BatchState`, the event is routed straight to DLQ with
`failure_reason = NO_CONTROL_DOC`. It is not retried — the control doc should
always arrive first.

---

### BatchProcessor

Main processing logic. Called by `EventBatchListener` with a
`List<ConsumerRecord>`.

Splits the list across the worker `ExecutorService` (8 threads). Each worker
handles a slice of the batch independently:

```
For each EventEnvelope in slice:
  │
  ├─► Deduplication check (idempotency_key in DeduplicationStore)
  │         HIT  → DUPLICATE, skip silently
  │         MISS → continue
  │
  ├─► Validate envelope fields (event_type, schema_id, trade_group_id present)
  │         Invalid → FatalException → DlqPublisher (FATAL)
  │
  ├─► trade_group_id has a registered BatchState?
  │         NO  → DlqPublisher (NO_CONTROL_DOC)
  │         YES → continue
  │
  ├─► Process payload (normalization, type coercion)
  │         TransientException → RetryPublisher (attempt_count+1)
  │         FatalException     → DlqPublisher (FATAL)
  │
  └─► Success
        → DeduplicationStore.mark(idempotency_key)
        → BatchCoordinator.recordProcessed(trade_group_id, trade_id)
        → result = SUCCESS
```

After all workers complete, `BatchProcessor` returns an aggregated
`BatchProcessorResult` to `EventBatchListener`, which commits the offset.

---

## Internal Architecture

```
banking-control topic
        │  batch poll
        ▼
ControlDocListener
        │  TradeDoc
        ▼
BatchCoordinator.register()
        │  creates BatchState { expectedCount, expectedTradeIds, deadline }
        │
        │               ┌─────────────────────────────┐
        │               │  Timeout Scheduler (every 30s)│
        │               │  → mark FAIL if now > deadline│
        │               └─────────────────────────────┘

banking-transactions topic
        │  batch poll (500 records)
        ▼
EventBatchListener
        │  List<ConsumerRecord>
        ▼
BatchProcessor
        │  splits across ExecutorService (8 workers)
        │
   Worker-1..N
        │  per event: dedup → validate → process → route
        ▼
   ┌────┴──────────────────┬──────────────────────┐
   ▼                       ▼                      ▼
BatchCoordinator      RetryPublisher          DlqPublisher
.recordProcessed()    (transient errors)      (fatal / no control doc /
   │                                           retry exhausted)
   ▼
actualCount == expectedCount
AND receivedTradeIds == expectedTradeIds?
   │
   YES ──► BatchState.status = SUCCESS
           StagingBuffer.add(all events in group)
   │
   NO  ──► remain PENDING until deadline
           (timeout scheduler marks FAIL)
   │
   ▼
Manual offset commit
(after all events in the poll are resolved)
```

---

## Package Structure

```
ingestion/
├── DESIGN.md
└── src/main/java/com/adaptivedata/ingestion/
    ├── config/
    │   ├── KafkaConsumerConfig.java      # listener factories, concurrency, ack mode
    │   ├── KafkaProducerConfig.java      # retry + DLQ topic producers
    │   └── IngestionConfig.java          # worker pool size, timeout, backoff, cache TTL
    ├── consumer/
    │   ├── BatchConsumer.java            # owns both Kafka listeners
    │   ├── ControlDocListener.java       # banking-control batch listener
    │   └── EventBatchListener.java       # banking-transactions batch listener
    ├── coordinator/
    │   ├── BatchCoordinator.java         # registers batches, tracks counts, runs timeout check
    │   └── BatchState.java              # per-trade-group state (expectedCount, receivedTradeIds, status, deadline)
    ├── processor/
    │   ├── BatchProcessor.java           # splits batch across worker pool, aggregates results
    │   ├── ErrorClassifier.java          # maps exception to TRANSIENT or FATAL
    │   └── DeduplicationStore.java       # Caffeine cache on idempotency_key (TTL 60 min)
    ├── staging/
    │   └── StagingBuffer.java            # LinkedBlockingQueue<ProcessedEvent>; Phase 2 → MinIO writer
    ├── routing/
    │   ├── RetryPublisher.java           # stamps attempt_count + retry_after headers
    │   └── DlqPublisher.java             # stamps failure_reason + original_topic headers
    └── model/
        ├── ProcessedEvent.java           # clean event ready for staging
        ├── BatchProcessorResult.java     # aggregated per-poll outcome
        ├── BatchStatus.java              # enum: PENDING / SUCCESS / FAIL
        └── EventResult.java             # per-event enum: SUCCESS / DUPLICATE / RETRY_ROUTED / DLQ_ROUTED
```

---

## Batch Validation Logic (BatchCoordinator)

Completion is declared only when **both** conditions are true:

```
actualCount == expectedCount
AND
receivedTradeIds.equals(expectedTradeIds)
```

Count alone is not sufficient — it would pass if different trade IDs arrived in
place of the expected ones. The set comparison catches substitution.

**FAIL conditions:**

| Condition | Outcome |
|---|---|
| `now > deadline` and batch still PENDING | `status = FAIL`, publish `BatchFailEvent` to DLQ |
| `actualCount > expectedCount` | impossible after dedup — log warning, continue |
| `actualCount == expectedCount` but sets differ | treated as PENDING until deadline (extra trade IDs may still arrive) |

---

## Error Routing

### Per-Event

| Error type | Examples | Route |
|---|---|---|
| Fatal | JSON parse fail, missing `trade_group_id`, bad schema_id | DLQ immediately |
| No control doc | Event arrives with unknown `trade_group_id` | DLQ (`NO_CONTROL_DOC`) |
| Transient | Network I/O, staging write lock | Retry topic (max 3 attempts) |
| Retry exhausted | Still failing after attempt 3 | DLQ (`RETRY_EXHAUSTED`) |
| Duplicate | `idempotency_key` already seen | Skip silently |

### Per-Batch (trade group level)

| Condition | Route |
|---|---|
| `SUCCESS` | All events flushed to `StagingBuffer` |
| `FAIL` / timeout | `BatchFailEvent` published to DLQ with `trade_group_id`, `expected_count`, `actual_count`, `missing_trade_ids` |

---

## Retry Backoff Contract

`RetryPublisher` stamps two headers on every retry event:

| Header | Type | Value |
|---|---|---|
| `attempt_count` | `int` | 1, 2, or 3 |
| `retry_after` | `long` (epoch ms) | `now + backoff_ms` |

| Attempt | Delay |
|---|---|
| 1 | 2 000 ms |
| 2 | 4 000 ms |
| 3 | 8 000 ms |

`RetryIngestionListener` reads `retry_after` on poll. If
`currentTimeMillis() < retry_after`, the event is re-queued and the listener
parks until the delay has elapsed. On attempt 3 failure, `DlqPublisher` is
called with `failure_reason = RETRY_EXHAUSTED`.

---

## Key Spring Kafka Configuration

```yaml
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    consumer:
      group-id: onprem-ingestion-group
      auto-offset-reset: earliest
      enable-auto-commit: false
      max-poll-records: 500
      fetch-min-size: 1048576       # 1 MB
      fetch-max-wait: 500           # ms
    listener:
      ack-mode: MANUAL_IMMEDIATE
      type: BATCH
      concurrency: 1                # must equal partition count

ingestion:
  worker-pool-size: 8
  dedup-cache-ttl-minutes: 60
  batch:
    timeout-minutes: 5
    timeout-check-interval-seconds: 30
  retry:
    max-attempts: 3
    backoff-base-ms: 2000
  topics:
    control: banking-control
    primary: banking-transactions
    retry: banking-transactions-retry
    dlq: banking-transactions-dlq
```

---

## Staging Buffer Contract (Phase 1)

`StagingBuffer` is a `LinkedBlockingQueue<ProcessedEvent>`. Events are only
added when `BatchCoordinator` marks a trade group `SUCCESS` — partial batches
never reach staging.

- A scheduled drain thread flushes the buffer to the intelligence layer every
  5 seconds or 1 000 events, whichever comes first.
- Phase 2 swaps the drain target from in-memory to MinIO with no changes to
  `BatchProcessor` or `BatchCoordinator`.

---

## Partition Scaling Guide

Start: **1 partition, concurrency = 1, 8 worker threads**.

When the listener thread is the bottleneck:

1. Increase topic partition count.
2. Increase `listener.concurrency` to match.
3. `DeduplicationStore`, `BatchCoordinator`, and `StagingBuffer` are shared
   singletons and remain thread-safe across all listener threads.

---

## Open TODOs

- Validation engine output contract (define before intelligence layer).
- Backpressure plan: consumer lag threshold and autoscaling policy.
- SLOs: ingestion latency p95, batch completion time p95, error budget.
- IAM/RBAC matrix for Kafka and S3/StorageGrid access.
- Compaction/small-file strategy for StorageGrid writes (Phase 2+).
