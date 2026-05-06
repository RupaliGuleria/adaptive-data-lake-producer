import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "banking-transactions")
CSV_FILE_PATH: str = os.getenv("CSV_FILE_PATH", "data/transactions.csv")
PRODUCER_BATCH_SIZE: int = int(os.getenv("PRODUCER_BATCH_SIZE", "500"))
PIPELINE_VERSION: str = os.getenv("PIPELINE_VERSION", "1.0.0")
