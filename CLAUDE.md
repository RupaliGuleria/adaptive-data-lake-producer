# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Status

Pre-implementation. Only `architecture.md` exists — a full PRD and architecture review. No code yet.

## System Overview

Event-driven hybrid streaming+batch **lakehouse platform** with two ingestion paths:

- **Cloud path**: Kafka → Google Dataflow → GCS (Parquet/Avro)
- **On-prem path**: Kafka → Java/Spark → S3/StorageGrid (Parquet/Protobuf)

Both paths pass through a central **intelligence layer** (schema engine → data quality engine → validation engine) before persistence. A shared query interface (DuckDB for lightweight, Spark for heavy) sits above storage, fronted by a REST API. Apache Hudi is a planned convergent format layer once the baseline parquet path is stable.

## Planned Repository Layout

```
producers/        # Cloud + on-prem event emitters
kafka/            # Topic config, retention, DLQ/retry topic definitions
ingestion/        # Cloud (Dataflow) and on-prem (Spark) adapters + shared normalization
intelligence/     # Schema engine, data quality engine, validation engine + versioned rulesets
staging/          # Transient storage abstraction with TTL controls
storage/          # Parquet writer, partitioner, format adapters (Avro/Protobuf)
query/            # DuckDB and Spark query adapters
api/              # REST contract and query orchestration
infra/            # Docker/Terraform for local + cloud
docs/             # Architecture decisions, contracts, runbooks
```

## Key Architectural Decisions (from architecture.md)

### Data Contracts
- Event envelope must include: `event_id`, `event_version`, `event_type`, `trace_id`, `idempotency_key`, `timestamp` (ISO-8601 UTC)
- Schema engine output must carry: `drift_detected`, `drift_type`, `breaking_change`, `compatibility` (backward|forward|full|none), `recommended_action`
- Data quality output must carry: `score`, `rule_id`, `severity` per issue, threshold policy (reject if score < 0.7)
- Final storage record must carry: `ingestion_time`, `pipeline_version`, `source_topic`, `validation_status`

### Reliability Semantics
- Kafka offset commit: **manual**, at-least-once minimum; effectively-once where feasible
- Dedup key: `event_id` or hash of normalized payload (must be explicit per producer)
- Invalid events route to a **DLQ topic** + optional retry topic
- Compaction target: **128MB files**, partitioned by `date/hour/producer_id`

### Query Layer
- DuckDB: lightweight, single-node queries
- Spark: heavy/cross-partition scans
- SQL dialect portability between engines is a known risk — define a supported function subset before implementing query adapters

### Open Design Questions (resolve before implementation)
1. Is ML an **external consumer** or **internal platform workload**?
2. Staging layer semantics: memory buffer vs object store vs table?
3. Schema registry: internal or external (e.g., Confluent)?
4. Validation engine output contract (only schema+quality outputs are currently specified)

## Implementation Priority Order

1. Freeze event envelope v1 → add contract tests
2. Define ingestion semantics (idempotency, offset commit, retry/DLQ)
3. Intelligence layer with versioned rules + drift policies
4. Partitioned Parquet writer with atomic publish + compaction hooks
5. Query API with engine routing and pagination
6. Observability (consumer lag, throughput, quality score trends, query p95)
7. Hudi integration (behind feature flag, after parquet path is stable)
