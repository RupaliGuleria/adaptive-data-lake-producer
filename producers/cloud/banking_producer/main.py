from __future__ import annotations

import logging
import sys
import time
import uuid

from .config import (
    CONTROL_DOC_LEAD_TIME_MS,
    CSV_FILE_PATH,
    ID_FIELD,
    PIPELINE_VERSION,
    PRODUCER_BATCH_SIZE,
    SCHEMA_ID,
)
from .csv_loader import load_transactions
from .producer import BankingProducer
from .schema import TradeDoc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def _trade_id(row: dict) -> str:
    tid = row.get(ID_FIELD)
    if tid is not None:
        return str(tid)
    raise ValueError(f"Row has no {ID_FIELD}: {row}")


def main() -> None:
    """Stream CSV rows into Kafka in trade-group chunks of PRODUCER_BATCH_SIZE."""
    rows = list(load_transactions(CSV_FILE_PATH))
    if not rows:
        logger.warning("No rows loaded from %s — nothing to send", CSV_FILE_PATH)
        return

    producer = BankingProducer()
    total_sent = 0

    for chunk_start in range(0, len(rows), PRODUCER_BATCH_SIZE):
        chunk = rows[chunk_start : chunk_start + PRODUCER_BATCH_SIZE]
        trade_group_id = str(uuid.uuid4())
        trade_ids = [_trade_id(r) for r in chunk]

        control_doc = TradeDoc(
            trade_group_id=trade_group_id,
            expected_count=len(chunk),
            trade_ids=trade_ids,
            schema_id=SCHEMA_ID,
            pipeline_version=PIPELINE_VERSION,
        )
        producer.send_control_doc(control_doc)
        producer.flush()
        logger.info(
            "Control doc sent | trade_group_id=%s expected_count=%d",
            trade_group_id,
            len(chunk),
        )

        if CONTROL_DOC_LEAD_TIME_MS > 0:
            # Ack above only proves Kafka persisted the control doc, not that
            # ControlDocListener has consumed it yet — this is an explicit,
            # opt-in extra margin on top of that, not a substitute for it.
            time.sleep(CONTROL_DOC_LEAD_TIME_MS / 1000)

        for row, trade_id in zip(chunk, trade_ids):
            producer.send(row, trade_group_id=trade_group_id, trade_id=trade_id)

        producer.flush()
        total_sent += len(chunk)
        logger.info("Progress | sent=%d", total_sent)

    logger.info("Complete | total_events=%d", total_sent)


if __name__ == "__main__":
    main()
