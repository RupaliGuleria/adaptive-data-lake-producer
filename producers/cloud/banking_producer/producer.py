from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Dict

from confluent_kafka import Producer

from .config import CONTROL_TOPIC, EVENT_TYPE, ID_FIELD, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, PIPELINE_VERSION, SCHEMA_ID
from .schema import EventEnvelope, TradeDoc

logger = logging.getLogger(__name__)


def _make_idempotency_key(row: Dict[str, Any]) -> str:
    """Build a stable deduplication key from a row, preferring the configured ID field."""
    if row.get(ID_FIELD) is not None:
        return str(row[ID_FIELD])
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
        self._control_topic = CONTROL_TOPIC
        logger.info("Producer connected | brokers=%s topic=%s", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)

    def send_control_doc(self, trade_doc: TradeDoc) -> None:
        """Publish a TradeDoc to the control topic before the event batch."""
        self._producer.produce(
            topic=self._control_topic,
            key=trade_doc.trade_group_id,
            value=trade_doc.model_dump_json(),
            callback=_delivery_report,
        )
        self._producer.poll(0)

    def send(self, row: Dict[str, Any], trade_group_id: str, trade_id: str) -> None:
        """Send one banking transaction row to Kafka without blocking."""
        envelope = EventEnvelope(
            event_type=EVENT_TYPE,
            idempotency_key=_make_idempotency_key(row),
            pipeline_version=PIPELINE_VERSION,
            schema_id=SCHEMA_ID,
            trade_group_id=trade_group_id,
            trade_id=trade_id,
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
