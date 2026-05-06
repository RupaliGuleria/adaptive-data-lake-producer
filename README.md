# Adaptive Data Lake

Adaptive Data Lake is an event-driven data lakehouse blueprint with an initial
cloud banking producer implementation. The current code reads banking
transactions from CSV(kaggle data), wraps every row in a versioned event envelope, and
publishes the events to Kafka for downstream ingestion, validation, and storage.

## Current Scope

- Cloud producer for banking transaction CSV files.
- Local Kafka and Kafka UI stack through Docker Compose.
- Standard event envelope with event IDs, trace IDs, idempotency keys,
  timestamps, source metadata, pipeline version, and payload.
- Architecture notes for the larger hybrid cloud/on-prem data lake platform in
  `architecture.md`.

## Repository Layout

```text
.
├── architecture.md
├── producers/
│   ├── docker-compose.yml
│   └── cloud/
│       ├── Dockerfile
│       ├── requirements.txt
│       ├── data/
│       │   └── transactions.csv
│       └── banking_producer/
│           ├── config.py
│           ├── csv_loader.py
│           ├── main.py
│           ├── producer.py
│           └── schema.py
```

## Architecture Summary

The intended platform uses Kafka as the event backbone. Producers publish
source events into Kafka, ingestion jobs consume those events, and an
intelligence layer performs schema checks, quality checks, and validation before
records are persisted to lake storage.

The planned cloud path is Kafka to Google Dataflow to GCS. The planned on-prem
path is Kafka to Java/Spark to S3-compatible storage. A shared query layer is
expected to support DuckDB for lightweight workloads and Spark for larger scans.

## Event Envelope

Every banking transaction is sent as an `EventEnvelope` with:

- `event_id`: unique event identifier.
- `event_version`: envelope contract version.
- `event_type`: logical event name, currently `banking_transaction`.
- `trace_id`: correlation ID for lineage and debugging.
- `idempotency_key`: stable deduplication key.
- `timestamp`: UTC event creation time.
- `source`: producer environment, currently `cloud`.
- `pipeline_version`: producer pipeline version.
- `payload`: raw normalized transaction fields from the CSV row.

## Prerequisites

- Docker and Docker Compose for local Kafka.
- Python 3.11 or newer for running the cloud producer locally.

## Dataset

Download the Indian Banking Transactions dataset from Kaggle and place the CSV
in `producers/cloud/data/transactions.csv`:

```bash
curl -L -o ~/Downloads/indian-banking-transactions-20192024.zip \
  https://www.kaggle.com/api/v1/datasets/download/belbino/indian-banking-transactions-20192024

unzip ~/Downloads/indian-banking-transactions-20192024.zip \
  -d producers/cloud/data/

# Rename to the expected filename if needed
mv producers/cloud/data/*.csv producers/cloud/data/transactions.csv
```

The `data/` directory is gitignored — the CSV is not committed to the repo.

## Local Setup

Start Kafka and Kafka UI:

```bash
cd producers
docker compose up -d
```

Kafka listens on `localhost:9092`. Kafka UI is available at
`http://localhost:8081`.

Create a `.env` file and activate a Python environment:

```bash
cd producers/cloud
cp .env.example .env          # edit if your CSV path or topic differs
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run the producer:

```bash
python3 -m banking_producer.main
```

Stop the Kafka stack when done:

```bash
cd producers
docker compose down
```

## Configuration

The producer reads configuration from environment variables, with defaults:

| Variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `banking-transactions` | Kafka topic to publish to |
| `CSV_FILE_PATH` | `data/transactions.csv` | Source CSV file |
| `PRODUCER_BATCH_SIZE` | `500` | Flush interval while sending rows |
| `PIPELINE_VERSION` | `1.0.0` | Version stamped on outgoing events |

## Docker Producer

The cloud producer can also be built as a container from `producers/cloud`:

```bash
docker build -t banking-producer .
```

When running in Docker on the same Compose network as Kafka, set
`KAFKA_BOOTSTRAP_SERVERS=kafka:29092` so the container reaches the internal
Kafka listener.

## Important Modules

- `schema.py`: defines the shared event envelope contract.
- `csv_loader.py`: reads CSV files and normalizes common banking column names.
- `producer.py`: creates Kafka messages from normalized rows and handles
  delivery callbacks.
- `main.py`: wires CSV loading and Kafka publishing into a batch-flushing
  command.
- `config.py`: centralizes environment-based producer configuration.

## Roadmap

The broader architecture still calls for ingestion services, schema governance,
data quality rules, validation outputs, storage writers, query adapters, REST
APIs, observability, and a future Apache Hudi integration path. See
`architecture.md` for the full design review and implementation priorities.
