# Adaptive Data Lake PRD & Architecture Review

## 1) Executive Summary

The PRD describes a solid **event-driven hybrid streaming+batch lakehouse-ready** platform with clear end-to-end flow from producers to API/consumers. The architecture is coherent and extensible, especially around:

- cloud + on-prem ingestion paths,
- an explicit intelligence layer (schema + quality + validation),
- storage format split by environment,
- a future-friendly Hudi integration path.

Key implementation risks are concentrated around:

- cross-environment schema governance,
- exactly-once/idempotency guarantees,
- data contract versioning,
- partition/file compaction strategy,
- and API/query consistency across engines.

---

## 2) Alignment Check: PRD vs Architecture Diagram

### Strongly aligned

- **Dual producer model** (Cloud + On-Prem) feeding Kafka.
- **Dual ingestion model** (Dataflow for cloud, Java/Spark for on-prem).
- **Central intelligence layer** before persistence.
- **Split storage backends** (GCS and S3/StorageGrid).
- **Query abstraction** over DuckDB and Spark.
- **REST API delivery** to downstream consumers.
- **Future Hudi** shown as a convergent path for both cloud and on-prem.

### Needs explicit clarification

- The machine-readable flow includes **API -> consumers -> ML(optional)** implicitly, but diagram shows ML after consumers. Clarify whether ML is:
  1. downstream external consumer, or
  2. internal platform workload reading from storage/query/API.
- Staging layer semantics are unspecified (memory vs object store vs table) and retention policies are missing.
- Validation engine output contract is not explicitly defined (only schema and quality outputs are detailed).

---

## 3) Data Contract Review

### Producer Event Schema

Current schema is flexible enough for heterogeneous sources. To make it production-safe:

- Add `event_version` to the top-level envelope.
- Add `event_type` and `trace_id` for lineage and routing.
- Define timestamp standard (`ISO-8601 UTC`) and accepted clock skew.
- Define optional `idempotency_key` for dedup semantics.

### Schema Engine Output

Good drift fields exist (`drift_detected`, `drift_type`). Add:

- `breaking_change: bool`
- `compatibility: backward|forward|full|none`
- `recommended_action` (drop/quarantine/auto-evolve/manual-review)

### Data Quality Output

Useful baseline with score + issues. Add:

- `rule_id` and `severity` per issue,
- explicit null handling policy,
- threshold policy (e.g., reject if score < 0.7).

### Final Storage Record

Include:

- `ingestion_time`,
- `pipeline_version`,
- `source_topic`,
- `validation_status`.

These improve reproducibility, replay debugging, and governance.

---

## 4) Module Contract Review

### Kafka Consumer

- Interface is fine but should include offset/partition metadata in returned Event.
- Define commit strategy: auto vs manual; at-least-once vs effectively-once.

### Ingestion Processor

- Dedup key strategy should be explicit (`event_id` or hash of normalized payload).
- Add invalid event handling path (DLQ topic + retry topic).

### Schema Engine

- Add schema registry integration contract (if external registry is used).
- Define evolution behavior per source/producer.

### Data Quality Engine

- Separate deterministic validation rules vs statistical anomaly checks.
- Make output deterministic for replay runs (versioned rulesets).

### Storage Writer

- Add transactional write guarantees and atomic partition publish behavior.
- Include compaction/small-file handling contract.

### Query Interface

- Add SQL dialect portability guidance between DuckDB and Spark.
- Define supported function subset to avoid drift between engines.

### API Layer

- Add pagination, sorting, and response schema standards.
- Add SLA targets and timeout behavior for Spark-backed queries.

---

## 5) Performance and Reliability Review

### What is strong

- Partition strategy (`date/hour/producer_id`) is a strong default.
- File size target (`128MB`) is aligned with efficient scan patterns.
- Query controls (predicate pushdown + column pruning) are correctly identified.

### What to add

- **Backpressure plan** in ingestion (consumer lag thresholds, autoscaling policy).
- **Retry policy** with bounded attempts + DLQ.
- **Compaction schedule** for late or micro-batched writes.
- **Hot partition mitigation** if producer skew is high.
- **SLOs**: ingestion latency, freshness, query p95, and error budget.

---

## 6) Security, Governance, and Operations Gaps

Recommended additions:

- Encryption at rest/in transit requirements.
- IAM/RBAC matrix per layer (producer, ingestion, query, API).
- PII tagging/redaction strategy in intelligence layer.
- Audit logs for schema changes and data quality overrides.
- Data retention + lifecycle rules for staging and storage.
- Disaster recovery objective definitions (RPO/RTO).

---

## 7) Suggested Repository Bootstrap Mapping

Given target layout:

- `producers/`: sample cloud/on-prem event emitters.
- `kafka/`: topic config, retention, DLQ/retry conventions.
- `ingestion/`: cloud and on-prem adapters + shared normalization.
- `intelligence/`: schema, quality, validation modules + rulesets.
- `staging/`: transient storage abstraction and TTL controls.
- `storage/`: parquet writer, partitioner, format adapters (avro/protobuf).
- `query/`: DuckDB and Spark query adapters.
- `api/`: REST contract and query orchestration.
- `infra/`: Docker/Terraform modules for local + cloud.
- `docs/`: architecture decisions, contracts, runbooks.

---

## 8) Prioritized Next Steps (Implementation Plan)

1. Freeze event envelope v1 and add contract tests.
2. Define ingestion semantics (idempotency, offset commit, retry/DLQ).
3. Implement intelligence layer with versioned rules and drift policies.
4. Implement partitioned parquet writer with atomic publish and compaction hooks.
5. Expose query API with engine routing and pagination.
6. Add observability dashboards (lag, throughput, quality score trends, query latency).
7. Add Hudi behind feature flag once baseline parquet path is stable.

---

## 9) Overall Assessment

The PRD and diagram form a strong architectural foundation. With explicit contracts for reliability, schema governance, and operational policies, this can move from concept to production-grade blueprint quickly.
