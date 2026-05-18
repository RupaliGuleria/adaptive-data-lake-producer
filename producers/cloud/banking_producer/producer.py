from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Dict

from confluent_kafka import Producer

from .config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, PIPELINE_VERSION, SCHEMA_ID
from .schema import EventEnvelope

logger = logging.getLogger(__name__)


def _make_idempotency_key(row: Dict[str, Any]) -> str:
    """Build a stable deduplication key from a transaction row."""
    if row.get("transaction_id") is not None:
        return str(row["transaction_id"])
    raw = json.dumps(row, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


def _delivery_report(err: Any, msg: Any) -> None:
    """Log Kafka delivery success or failure from the producer callback."""
    if err:
        logger.error("Delivery failed | key=%s error=%s", msg.key(), err)
    else:
        logger.debug(
            "Delivered | topic=%s partition=%d offset=%d",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


class BankingProducer:
    """Kafka producer that publishes normalized banking rows as event envelopes.

    The class owns the Confluent Kafka producer configuration, converts each
    CSV row into the shared `EventEnvelope`, and flushes messages in batches so
    the local producer can be used safely by scripts or containers.
    """

    def __init__(self) -> None:
        self._producer = Producer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "acks": "all",
                "retries": 3,
                "enable.idempotence": True,
            }
        )
        self._topic = KAFKA_TOPIC
        logger.info("Producer connected | brokers=%s topic=%s", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)

    def send(self, row: Dict[str, Any]) -> None:
        """Send one banking transaction row to Kafka without blocking."""
        envelope = EventEnvelope(
            event_type="banking_transaction",
            idempotency_key=_make_idempotency_key(row),
            pipeline_version=PIPELINE_VERSION,
            schema_id=SCHEMA_ID,
            payload=row,
        )
        self._producer.produce(
            topic=self._topic,
            key=envelope.event_id,
            value=envelope.model_dump_json(),
            callback=_delivery_report,
        )
        # Non-blocking poll to trigger delivery callbacks without stalling
        self._producer.poll(0)

    def flush(self) -> None:
        """Wait for buffered Kafka messages to be delivered."""
        pending = self._producer.flush(timeout=30)
        if pending:
            logger.warning("%d message(s) still pending after flush", pending)
