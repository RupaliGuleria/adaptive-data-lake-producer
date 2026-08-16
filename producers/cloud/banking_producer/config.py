import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "banking-transactions")
CONTROL_TOPIC: str = os.getenv("CONTROL_TOPIC", "banking-control")
CSV_FILE_PATH: str = os.getenv("CSV_FILE_PATH", "data/transactions.csv")
PRODUCER_BATCH_SIZE: int = int(os.getenv("PRODUCER_BATCH_SIZE", "500"))
PIPELINE_VERSION: str = os.getenv("PIPELINE_VERSION", "1.0.0")
SCHEMA_ID: str = os.getenv("SCHEMA_ID", "banking_transaction_v1")

# Payload field used as the per-row unique key (trade_id / dedup key). Must be
# present and unique in every row of CSV_FILE_PATH — e.g. "o_orderkey" for
# TPC-H orders, "line_item_id" for TPC-H lineitem.
ID_FIELD: str = os.getenv("ID_FIELD", "transaction_id")

# EventEnvelope.event_type — a free-text label for the payload's domain.
EVENT_TYPE: str = os.getenv("EVENT_TYPE", "banking_transaction")
