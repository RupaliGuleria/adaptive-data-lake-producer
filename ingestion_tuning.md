# Ingestion tuning: the control-doc race, the drain-cap ceiling, and the writer-pool redesign

> Session date: 2026-08-22. Covers everything done to the ingestion service's
> throughput/reliability between the SF 0.1 TPC-H load (`tpch_ingestion.md`)
> and Week 3 of `lakehouse-scheduler-research`. For pipeline architecture see
> `flow.md` / `lakehouse-scheduler-research/docs/PIPELINE_ARCHITECTURE.md`.
> **Not done yet, and needed for Week 3**: a rate-limited producer (slow/fast/
> heavy_burst tiers) — see §6. Don't skip that section.

---

## 0. Why this happened

While kicking off SF 0.1 ingestion for Week 3, throughput was far worse than
expected and a large fraction of events were hitting the retry path
(`NO_CONTROL_DOC`). Three separate problems turned out to be stacked on top
of each other. Fixing them one at a time, in the order discovered:

1. A blocking `Thread.sleep()` in the retry-topic consumer.
2. A race between the control-doc consumer and the event consumer.
3. A hardcoded staging-drain cap that limited throughput to ~200 events/sec
   **regardless of anything else** — this was the real ceiling the whole
   time; the first two fixes just removed things that were even worse than it.

All of this was diagnosed and fixed against **local** Kafka + local MinIO,
with a `mc.exe` local cleanup step afterward — the real Ryzen dataset
(`hive.events.raw_events`, SF 0.1: 150,000 `tpch_orders_v1` + 600,572
`tpch_lineitem_v1`) was never touched by any of this and remains intact.

---

## 1. Problem 1 — retry listener blocked the Kafka consumer thread

**Symptom**: `RetryIngestionListener.onRetryEvents()` called
`Thread.sleep(waitMs)` synchronously, per batch, to honor the retry
backoff (2s → 4s → 8s). With `spring.kafka.listener.concurrency: 1` and the
retry topic having exactly one partition, there is only ever one consumer
thread for that topic — so every retry batch's backoff fully serialized
behind whichever batch was sleeping.

**First attempt (wrong, kept for the record)**: replaced the sleep with
`ScheduledExecutorService.schedule()` — one scheduled task **per record**.
This removed the blocking, but processed retries one at a time instead of
in the original design's efficient batches. Measured: 18,815 individual
`Delayed retry processed` dispatches vs. only 60 real batch polls, and
throughput was no better than the blocking version (~177.7 events/sec
either way). Lesson: removing a block isn't enough if you also destroy the
batching that made the original path fast.

**Fix that actually worked** — `RetryIngestionListener.java`: a
`DelayQueue<PendingRetry>` (a `record` implementing `Delayed`) drained by
one dedicated thread that blocks on `take()` (no busy-wait), then
`drainTo()` to sweep up everything else already due at that moment, and
processes the whole batch through the **existing** 8-worker
`BatchProcessor.process()` — not one at a time.

```java
private void drainLoop() {
    while (running) {
        PendingRetry first = pending.take();
        List<PendingRetry> due = new ArrayList<>();
        due.add(first);
        pending.drainTo(due);
        // ... batch process, not one-at-a-time
    }
}
```

**Durability tradeoff, explicit**: the Kafka offset is acked the moment a
retry record is queued in-memory, not once it's actually processed. A JVM
crash in that narrow window (record queued, backoff not yet elapsed) loses
it from the durable log. Accepted for a research pipeline; closing it fully
would need a durable delay mechanism (e.g. re-publish to Kafka with a
delay), out of scope.

---

## 2. Problem 2 — the control-doc race itself

**Root cause**: `ControlDocListener` (topic `banking-control`) and
`EventBatchListener` (topic `banking-transactions`) are independent
consumers with no ordering guarantee between them. The producer already
does the right thing — `send_control_doc()` then `flush()` (blocks for the
Kafka broker's ack) *before* sending that chunk's events — but a broker ack
only proves persistence, not that `ControlDocListener` has polled and
registered it in `BatchCoordinator` yet.

**The actual fixable cause**: the shared Kafka consumer factory used
`fetch.min.bytes = 1MB`, tuned for the bulk `banking-transactions` topic.
`banking-control` carries tiny individual JSON messages that almost never
reach 1MB, so **every poll on that topic was waiting out the full
`fetch.max.wait.ms = 500` regardless of whether a control doc was already
sitting there** — a self-inflicted several-hundred-ms lag on exactly the
consumer that needed to be fast.

**Fix** — `KafkaConsumerConfig.java`: a second consumer factory
(`controlConsumerFactory` / `controlListenerContainerFactory`) with
`fetch.min.bytes = 1`, `fetch.max.wait.ms = 50`. `ControlDocListener` points
at it; the primary/retry topics' factory is untouched.

```yaml
spring:
  kafka:
    control-consumer:
      fetch-min-size: 1
      fetch-max-wait: 50
```

**Producer-side knob added, but off by default** — `producer/config.py` /
`main.py`: `CONTROL_DOC_LEAD_TIME_MS` (default `0`), an optional extra
`time.sleep()` after the control-doc flush and before sending events.
Documented explicitly as *not* a substitute for the ack — just an opt-in
extra margin. Not needed once the consumer-side fix landed.

**Result** (75,175-row SF 0.01 backup burst, scratch bucket, real Ryzen):

| | Before | After |
|---|---|---|
| Retry records | 18,815 (25.0%) | **0 (0.00%)** |
| Throughput | ~177.7 events/sec | ~247.9 events/sec |
| Quarantined | 0 | 0 |

---

## 3. Problem 3 — Parquet writes blocking the shared scheduler (partial fix)

Every `Parquet written` / `PASSED → Parquet buffer` log line ran on thread
`scheduling-1`. Spring Boot's `@Scheduled` defaults to a **single-threaded**
executor unless configured otherwise — nothing in this codebase had raised
it. That one thread did everything downstream of Kafka: drain, validate,
buffer, Parquet-compress, and the blocking MinIO `PutObject` call.

Profiling (`parquet_write_ms` / `s3_put_ms`, added to
`ParquetStorageWriter`) showed the real cost was **compression, not
network**: `parquet_write_ms=2290` vs `s3_put_ms=243` for a 7,000-event
file. Fix: `ParquetWriteBuffer` got a dedicated writer pool (`writerPool`,
now `writer-threads` in config), and `spring.task.scheduling.pool.size`
was raised to 4 for the remaining lightweight scheduled tasks.

**This fix was necessary but not sufficient** — throughput barely moved
(~194-253 events/sec) even with writes confirmed running in parallel on
`parquet-io` threads. That pointed at a different, bigger bottleneck — §4.

---

## 4. Problem 4 (the real one) — `StagingBuffer`'s hardcoded drain cap

```java
private static final int DRAIN_BATCH_SIZE = 1_000;
@Scheduled(fixedDelay = 5000)
public void drain() {
    List<ProcessedEvent> batch = new ArrayList<>(DRAIN_BATCH_SIZE);
    buffer.drainTo(batch, DRAIN_BATCH_SIZE);
    // ...
}
```

**1,000 events every 5 seconds = a hard 200 events/sec ceiling**,
completely independent of Kafka consumption speed, validation speed, or
Parquet write speed. Every throughput number measured in §1-§3 — 177.7,
247.9, 194-253 — was landing suspiciously close to this exact number
because **this was the actual binding constraint the whole time**. The
earlier fixes removed things that were worse than 200/sec; nothing had yet
removed the 200/sec ceiling itself.

---

## 5. The redesign — continuous writer pool, no scheduled drain

A full design was proposed for removing the drain cap (continuous writer
threads, configurable target file size, hash-routed "writer lanes" by
partition key, flush-on-trade-group-completion, bounded queue). It was
reviewed critically before implementing — **agreed with most of it,
explicitly disagreed with two mechanisms**, and only the agreed part was
built.

### Agreed and implemented

- **No fixed drain interval.** `StagingBuffer` now runs one dedicated
  consumer thread: `queue.poll(200ms)` → `drainTo()` whatever's
  immediately available → process → loop. No wait interval limits normal
  throughput; efficient blocking (not busy-wait) when the queue is empty.
- **Byte-size-based flush, not row count.** `ParquetWriteBuffer` flushes a
  partition when its *estimated* buffered bytes reach
  `target-file-size-mb` (default 32), not a fixed event count. Estimate is
  derived from each partition's own last real flush
  (`actualBytes / recordCount`), falling back to a 200-bytes/record guess
  for partitions with no history yet. Estimated and actual (post-Snappy-
  compression) bytes are tracked and logged **separately** — they diverge
  meaningfully; don't assume 32MB buffered ≈ 32MB file.
- **Idle-flush timer** (`idle-flush-ms`, default 5000): a partition with no
  new events for that long flushes its partial buffer regardless of size,
  so low-traffic partitions don't sit unwritten indefinitely. Implemented
  as one remaining lightweight `@Scheduled` sweep — explicitly *not* the
  mechanism controlling normal data movement, just residual cleanup.
- **Bounded staging queue with real backpressure.** `StagingBuffer`'s queue
  was unbounded before (`LinkedBlockingQueue<>()`, no capacity) — a latent
  risk. Now bounded (`ingestion.staging.queue-capacity`, default 200,000),
  and `add()`/`addAll()` use blocking `put()` — a slow writer stage now
  visibly backs up the Kafka worker pool instead of silently growing
  memory or dropping events.
- **Graceful shutdown.** `ParquetWriteBuffer` already had a shutdown hook;
  `StagingBuffer` didn't — a real gap, since anything queued there at JVM
  exit was silently lost. Both now drain and flush everything queued
  before the process exits (`@PreDestroy`, bounded `awaitTermination`).
- **Configurable writer-thread count** (`writer-threads`, default 8) and
  **rich metrics** — see §7.

### Disagreed and skipped, deliberately

- **Hash-routed fixed writer "lanes"** (`lane = hash(partitionKey) %
  writerCount`). Rejected because the correct minimal grouping key for a
  Parquet file is already the Hive partition path
  (`year/month/day/hour/source`) — per
  `lakehouse-scheduler-research/docs/PIPELINE_ARCHITECTURE.md`, files
  already mix `schema_id`/`trade_group_id` freely within a partition, by
  design, so there's no finer correctness boundary to add. Fixed
  hash-routing by that same key would starve most of the pool whenever a
  run's traffic lands in one or two partitions — which is **exactly what
  every test burst in this pipeline actually does** (all of one hour's
  worth of data lands in a single partition). Implemented as designed, the
  writer-count experiment in §8 would have shown *zero* difference between
  1 and 8 threads on our own test data. Built instead: partition buffering
  unchanged (already correct) + a shared work queue (`ExecutorService`)
  where any free writer thread picks up whichever partition's buffer is
  next ready — genuine parallelism that doesn't depend on partition
  cardinality.
- **Flush on trade-group (control-doc) completion.** Rejected because it
  conflates two unrelated completion concepts — a trade group's Kafka
  delivery finishing vs. a partition's file being ready to write — and
  would produce small, premature files while a partition is still actively
  receiving other trade groups, undermining the whole point of buffering
  for bulk efficiency. The idle-flush timer already covers "don't hold
  data forever" without this special case.

### Files changed (all in `ingestion/src/main/java/com/adaptivedata/ingestion/`)

| File | Change |
|---|---|
| `config/KafkaConsumerConfig.java` | added `controlConsumerFactory` / `controlListenerContainerFactory` |
| `config/IngestionConfig.java` | added `StagingProperties` (`queueCapacity`, default 200,000) |
| `consumer/ControlDocListener.java` | points at the new low-latency factory |
| `consumer/RetryIngestionListener.java` | `Thread.sleep()` → `DelayQueue` + dedicated batch-draining thread |
| `consumer/EventBatchListener.java` | records `totalReceived` metric at the true entry point (not on retries) |
| `processor/BatchProcessor.java` | wired `IngestionMetrics` into every outcome branch |
| `staging/StagingBuffer.java` | scheduled drain → continuous consumer thread; bounded queue + blocking backpressure; shutdown drain |
| `intelligence/storage/ParquetProperties.java` | `targetFileSizeMb` (32), `maxEventsPerFileSafetyCap` (200,000, hard ceiling not normal trigger), `idleFlushMs` (5000), `writerThreads` (8) |
| `intelligence/storage/ParquetWriteBuffer.java` | per-partition byte-size buffering, idle sweep, shared writer pool (no hash lanes), graceful shutdown |
| `intelligence/storage/ParquetStorageWriter.java` | returns `WriteResult(actualBytes, writeMs, putMs)`; timing instrumentation |
| `metrics/IngestionMetrics.java` | new — see §7 |
| `producer/config.py`, `producer/main.py` | `CONTROL_DOC_LEAD_TIME_MS` (default 0, opt-in) |
| `application.yml` | all of the above config keys |

### Current `application.yml` reference

```yaml
spring:
  task:
    scheduling:
      pool:
        size: 4
  kafka:
    control-consumer:
      fetch-min-size: 1
      fetch-max-wait: 50

ingestion:
  metrics:
    log-interval-ms: 15000
  staging:
    queue-capacity: 200000
  intelligence:
    parquet:
      target-file-size-mb: 32
      max-events-per-file-safety-cap: 200000
      idle-flush-ms: 5000
      writer-threads: 8
```

All of the above are Spring `@ConfigurationProperties` — overridable via
env vars for quick experiments without recompiling, e.g.
`INGESTION_INTELLIGENCE_PARQUET_WRITERTHREADS=4`,
`INGESTION_INTELLIGENCE_PARQUET_TARGETFILESIZEMB=1`. Relaxed binding rule:
`ingestion.intelligence.parquet.writer-threads` →
`INGESTION_INTELLIGENCE_PARQUET_WRITERTHREADS`.

---

## 6. NOT built yet — rate-limited producer (slow / fast / heavy_burst)

**This is the thing most likely to matter for Week 3 and isn't done.**
`lakehouse-scheduler-research/research-plan/week3_testing_plan.md` §3 says
explicitly:

> Skip "moderate" tier for now — Checkpoint 1 only tested none vs. heavy; a
> moderate tier needs a rate-limited producer, which doesn't exist yet.

Design discussed and agreed in principle, never implemented:

- One new env var on the producer, e.g. `PRODUCER_TARGET_EVENTS_PER_SEC`
  (default `0` = unlimited, matching the existing "off unless set"
  pattern used for `CONTROL_DOC_LEAD_TIME_MS`).
- Sleep-based pacing in `main.py`'s event-sending loop — after each event
  or small sub-batch, sleep enough to hit the target rate. Simplest
  approach: track elapsed time vs. events sent so far, sleep the
  difference between actual and expected elapsed time at intervals (not
  every single event, to avoid excessive `sleep()` call overhead).
- Three tiers, roughly:
  - **slow** — low steady rate (e.g. ~50-100 events/sec), sustained.
  - **fast** — high steady rate, near the pipeline's real unthrottled
    ceiling (now much higher post-redesign than the old ~200/sec — real
    Ryzen ceiling not yet re-measured, see §9).
  - **heavy_burst** — unthrottled, concentrated in a short window — this
    is just today's default (no pacing) behavior, needs no new code.
- The harness's `checkpoint1_timing.csv` already has an `ingestion_rate`
  column (currently just a free-text label via `--ingestion-rate`); this
  would make it a real, reproducible, numeric parameter instead of a label.

**This should be built before or alongside Week 3's dataset-generation
pass** (`week3_testing_plan.md` §3's "active" ingestion condition) — not
optional infrastructure, it's the mechanism that condition depends on.

---

## 7. Metrics now available (`IngestionMetrics`, logged every `metrics.log-interval-ms`)

Two log lines per interval:

```
Ingestion metrics | total=... processed_normally=... retry_routed_control_doc=...
  retry_routed_other=... retry_recovered=... duplicates=... quarantined_control_doc=...
  quarantined_other=... retry_pct=...% elapsed_sec=... events_per_sec=...
  staging_queue_depth=... staging_queue_peak_depth=...

Parquet writer summary | writer_threads=... files_written=... files_failed=...
  records_per_file_mean=... records_per_file_median=... estimated_bytes_mean=...
  actual_bytes_mean=... actual_bytes_median=... parquet_write_ms_mean=...
  parquet_write_ms_median=... s3_put_ms_mean=... s3_put_ms_median=...

Parquet writer stat | thread=parquet-writer-N files=... records=...   (one line per writer thread)
```

`retry_routed_control_doc` / `quarantined_control_doc` are split from
`_other` specifically so `retry_count/total_events` for the control-doc
race can be measured in isolation from unrelated retry causes (parse
errors, generic retry exhaustion).

---

## 8. Local tuning sweep — results and the critical caveat

Run entirely on **this laptop's local Kafka + local MinIO**
(`localhost:9092`, `localhost:9000`, `minioadmin`/`minioadmin` —
different credentials from Ryzen's `minioadmin`/`minioadmin123`), using the
15,000-row SF 0.01 `orders.csv` backup
(`producers/cloud/data/tpch/sf0.01_backup/orders.csv`). Zero Ryzen/Tailscale
involvement. Config overridden per run via env vars, no recompiling.

### Writer-count sweep (forced 500-event safety cap → 30 files every run, so parallelism is actually observable)

| Writers | Elapsed (first→last flush) | Distribution | write_ms mean |
|---|---|---|---|
| 1 | ~5.40s | all 30 on writer-0 | 245.6 |
| 2 | ~3.49s | 10/10 | 430.9 |
| **4** | **~2.05s (best)** | 7/7/8/8 | 649.8 |
| 8 | ~2.92s (regression) | 3-4 each | 1166.2 |

4 writer threads was the local sweet spot; 8 oversubscribed this laptop's
CPU cores (per-task write time nearly 5x'd, and wall-clock time got
*worse*, not better). Distribution was well-balanced at every thread
count — no routing skew, confirming the shared-work-queue design (§5)
works as intended.

### File-size sweep (`target-file-size-mb`, writer-threads=8, same 15k-row / ~2.33MB-compressed dataset)

| Target | Files | Records/file (mean) |
|---|---|---|
| 1MB | 3 | ~5,000 |
| 2MB | 2 | ~7,500 |
| 4MB | 1 | 15,000 (whole dataset fit under threshold) |

Confirms the byte-size trigger and the actual-bytes feedback loop work
correctly. Does **not** tell us anything about which of 8/32/64/128MB is
actually best in production — the test data was deliberately too small to
reach those thresholds meaningfully (proportionally scaled tiers were used
instead, by design — see the AskUserQuestion exchange in the original
session for why literal 8-128MB needs real production-scale data).

### The caveat that matters most

**These numbers are local-machine relative comparisons only.** Zero
Tailscale network latency, this laptop's specific core count — not
directly comparable in absolute terms to the Ryzen-based baselines (177.7 /
247.9 / ~200-250 events/sec). A network-bound bottleneck (Ryzen) rewards
more concurrent writers differently than a CPU-bound one (local) does —
Ryzen's real optimum could plausibly be *higher* than 4, not the same.
**`writer-threads: 4` and `target-file-size-mb: 32` are provisional
defaults, not validated production settings.**

---

## 9. Next steps, in priority order

1. **Build the rate-limited producer** (§6) — genuinely blocks Week 3's
   dataset-generation pass, not optional.
2. Proceed with Week 3's own testing plan (query classes, baselines,
   prediction dataset) — the SF 0.1 dataset is already there and clean; no
   re-ingestion is currently needed to start this.
3. **Defer** re-running the writer-count/file-size tuning against real
   Ryzen scale + latency until there's an actual need to re-ingest
   (e.g. a larger scale factor per the paper plan's staged sizing table, or
   if Week 3/4 experiments reveal ingestion throughput is itself a
   bottleneck for the scheduler work). Don't manufacture a re-ingest just
   to benchmark — combine it with real, needed data generation when that
   happens.

---

## 10. Infra state as of end of this session (2026-08-22)

- **Ryzen**: up, reachable (~8ms ping). Trino, Hive Metastore, Postgres,
  MinIO all confirmed healthy via a live query. MinIO's S3 API (`:9000`)
  is now directly reachable from the research laptop over Tailscale — it
  wasn't earlier in this same session (port reachability changed
  mid-session, cause unknown, worth re-checking if it regresses).
- **`hive.events.raw_events`**: `tpch_orders_v1` = 150,000,
  `tpch_lineitem_v1` = 600,572, 0 quarantined. Clean SF 0.1 dataset,
  verified intact after all of the above.
- **Ryzen MinIO buckets**: only `lakehouse-events` (real data) and
  `warehouse` (Hive Metastore infra) remain. All scratch/profile buckets
  created during this session's fix verification
  (`lakehouse-events-scratch`, `-scratch-v2`, `-profile`, `-profile-v2`)
  were deleted via boto3 from the research laptop.
- **Local** (this laptop): Kafka, Zookeeper, MinIO, Kafka UI all up via
  `adaptive-data-lake-producer/producers/docker-compose.yml` (6+ days).
  Local MinIO's `local-tuning-test` scratch bucket deleted; the
  pre-existing `adaptive-data-lake` bucket (dated 2026-08-16, unrelated to
  this session) was left untouched. Local Java ingestion service is **not
  currently running** — start it fresh (`mvn spring-boot:run` from
  `ingestion/`, env vars per `ingestion-pipeline/README.md`) before any
  new ingestion work.
- **Week 3 work itself**: not started. `scripts/query_timing_harness.py`
  still only has the `medium` query (`tpch_orders_sum`, unchanged from
  Checkpoint 1) — `short` and `long` from
  `research-plan/week3_testing_plan.md` §1 still need to be added and
  validated against real data before anything else in the testing plan
  can proceed.
