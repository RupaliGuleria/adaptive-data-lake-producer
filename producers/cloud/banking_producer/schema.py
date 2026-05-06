from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict

from pydantic import BaseModel, Field


def _utc_now() -> str:
    """Return the current UTC time in ISO-8601 format for event timestamps."""
    return datetime.now(timezone.utc).isoformat()


def _new_uuid() -> str:
    """Create a fresh UUID string for event and trace identifiers."""
    return str(uuid.uuid4())


class EventEnvelope(BaseModel):
    """Standard Kafka event contract used by the adaptive data lake pipeline.

    The envelope keeps routing, lineage, idempotency, and versioning metadata
    separate from the raw banking transaction payload. Downstream ingestion,
    validation, and storage components can rely on these top-level fields even
    when individual producer payloads evolve over time.
    """

    event_id: str = Field(default_factory=_new_uuid)
    event_version: str = "1.0"
    event_type: str
    trace_id: str = Field(default_factory=_new_uuid)
    idempotency_key: str
    timestamp: str = Field(default_factory=_utc_now)
    source: str = "cloud"
    pipeline_version: str = "1.0.0"
    payload: Dict[str, Any]
