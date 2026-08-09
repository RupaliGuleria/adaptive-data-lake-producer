# Adaptive Data Lake

## What is this?

A small, working data lakehouse — end to end. It takes raw banking transaction
data, streams it through Kafka, checks its schema and quality in-flight,
stores the good data as Parquet files in S3-compatible storage, and lets you
query that data over a REST API.

The point of the project is to demonstrate a realistic ingestion → validation
→ storage → query pipeline, including the parts that are usually skipped in
toy examples: batch tracking, deduplication, retries, dead-letter handling,
schema drift detection, and data quality scoring.

## What it does, in one line

```
CSV  →  Kafka  →  Ingestion (dedup + batching)  →  Intelligence (schema + quality checks)
     →  Parquet in MinIO  →  Query via DuckDB  →  REST API
```

## Current status

| Layer | Status |
|---|---|
| Producer (Python, reads CSV → Kafka) | ✅ Built |
| Kafka + MinIO infrastructure (Docker Compose) | ✅ Built |
| Ingestion (Spring Boot — batching, dedup, retry/DLQ) | ✅ Built |
| Intelligence (schema drift + quality scoring) | ✅ Built |
| Storage (Parquet writer → MinIO) | ✅ Built |
| Query engine (DuckDB over Parquet) | ✅ Built |
| REST API (pagination, validation, standard error format) | ✅ Built |
| Observability (Prometheus/Grafana metrics) | 🔲 Not started |
| Apache Hudi integration | ⏸ Deferred |

Full task-by-task tracker: [`TASKS.md`](TASKS.md).
Full pipeline diagram + every data model: [`flow.md`](flow.md).
Design rationale and trade-offs: [`architecture.md`](architecture.md).

## How the pipeline works

1. The **Python producer** reads the transactions CSV, groups rows into
   batches of 500, and publishes a control message (what's coming) followed
   by the individual transaction events to Kafka.
2. The **ingestion service** (Java/Spring Boot) consumes both, matches events
   to their batch, deduplicates, and waits until a batch is complete before
   passing it on.
3. The **intelligence layer** checks each event against a schema (has the
   shape changed?) and a set of quality rules (missing fields, bad dates,
   negative amounts, etc.), giving it a score and a verdict: PASSED,
   QUARANTINED, or REJECTED.
4. PASSED and QUARANTINED events are written as **Parquet files** to MinIO
   (an S3-compatible object store); REJECTED events go to a dead-letter Kafka
   topic for inspection.
5. The **REST API** (`/api/v1/query`) lets you query that Parquet data
   on-demand — filter by date/source, paginate through results, all backed by
   DuckDB reading straight from MinIO.

## Repository layout

```text
.
├── README.md
├── architecture.md            — design review and trade-offs
├── flow.md                    — full pipeline diagram + data models
├── TASKS.md                   — task tracker (done / pending / deferred)
│
├── producers/                 — Kafka + MinIO infra, and the Python producer
│   ├── docker-compose.yml
│   └── cloud/
│       └── banking_producer/  — reads CSV, publishes to Kafka
│
└── ingestion/                 — Java/Spring Boot service: everything after Kafka
    ├── DESIGN.md
    └── src/main/java/com/adaptivedata/ingestion/
        ├── consumer/          — Kafka listeners
        ├── coordinator/       — tracks batches until complete
        ├── processor/         — dedup + per-event pipeline
        ├── routing/           — retry / dead-letter publishing
        ├── staging/           — buffer between ingestion and intelligence
        ├── intelligence/      — schema drift + quality scoring
        │   ├── schema/
        │   ├── quality/
        │   ├── routing/
        │   └── storage/       — Parquet writer → MinIO
        ├── query/             — DuckDB query engine + REST controller
        └── web/                — standardised API error responses
```

## Prerequisites

- Docker and Docker Compose (runs Kafka, Kafka UI, and MinIO)
- Python 3.11+ (producer)
- Java 21 + Maven (ingestion service)

## Quick start

**1. Start infrastructure** (Kafka on `:9092`, Kafka UI on `:8081`, MinIO on `:9000` / console `:9001`):

```bash
cd producers
docker compose up -d
```

**2. Start the ingestion + intelligence + query service** (REST API on `:8080`):

```bash
cd ingestion
mvn spring-boot:run
```

**3. Get the dataset and run the producer** — see [Dataset](#dataset) below, then:

```bash
cd producers/cloud
cp .env.example .env          # edit if your CSV path or topic differs
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 -m banking_producer.main
```

**4. Query the data** once events have flowed through:

```bash
curl "http://localhost:8080/api/v1/query?prefix=data&year=2026&month=7&pageSize=20"
```

Stop everything with `cd producers && docker compose down`.

## Dataset

Download the Indian Banking Transactions dataset from Kaggle and place the
CSV in `producers/cloud/data/transactions.csv`:

```bash
curl -L -o ~/Downloads/indian-banking-transactions-20192024.zip \
  https://www.kaggle.com/api/v1/datasets/download/belbino/indian-banking-transactions-20192024

unzip ~/Downloads/indian-banking-transactions-20192024.zip \
  -d producers/cloud/data/

# Rename to the expected filename if needed
mv producers/cloud/data/*.csv producers/cloud/data/transactions.csv
```

The `data/` directory is gitignored — the CSV is not committed to the repo.

## Configuration

**Producer** (`producers/cloud/.env`):

| Variable                  | Default                 | Purpose                            |
| ------------------------- | ------------------------ | ---------------------------------- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092`        | Kafka broker address               |
| `KAFKA_TOPIC`             | `banking-transactions`  | Kafka topic to publish to          |
| `CSV_FILE_PATH`           | `data/transactions.csv` | Source CSV file                    |
| `PRODUCER_BATCH_SIZE`     | `500`                    | Rows per trade group / batch flush |
| `PIPELINE_VERSION`        | `1.0.0`                  | Version stamped on outgoing events |

**Ingestion service** (`ingestion/src/main/resources/application.yml`) —
Kafka topics, batch timeout, retry/backoff, quality threshold, Parquet flush
settings, MinIO credentials, and query page-size limits all live there with
sane defaults for local development.

## Docker producer image

The producer can also run as a container from `producers/cloud`:

```bash
docker build -t banking-producer .
```

When running on the same Compose network as Kafka, set
`KAFKA_BOOTSTRAP_SERVERS=kafka:29092` so the container reaches the internal
listener.

## What's next

**Phase 9 — Observability**: Actuator + Prometheus metrics (events
ingested/DLQ'd, staging queue depth, quality score distribution, query
latency) and Grafana dashboards. See `TASKS.md` for the full list.

Longer-term ideas: Apache Hudi for upserts, and extending the intelligence
layer's schema/quality rules beyond banking (e.g. investment or medical data)
via externalized YAML rulesets instead of hardcoded Java rules.
