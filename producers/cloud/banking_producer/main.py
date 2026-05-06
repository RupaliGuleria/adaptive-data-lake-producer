from __future__ import annotations

import logging
import sys

from .config import CSV_FILE_PATH, PRODUCER_BATCH_SIZE
from .csv_loader import load_transactions
from .producer import BankingProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Stream configured CSV rows into Kafka in producer-sized batches."""
    producer = BankingProducer()
    count = 0

    for row in load_transactions(CSV_FILE_PATH):
        producer.send(row)
        count += 1
        if count % PRODUCER_BATCH_SIZE == 0:
            producer.flush()
            logger.info("Progress | sent=%d", count)

    producer.flush()
    logger.info("Complete | total_events=%d", count)


if __name__ == "__main__":
    main()
