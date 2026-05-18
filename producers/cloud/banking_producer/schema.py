from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_uuid() -> str:
    return str(uuid.uuid4())


class EventEnvelope(BaseModel):
    event_id: str = Field(default_factory=_new_uuid)
    event_version: str = "1.0"
    event_type: str
    trace_id: str = Field(default_factory=_new_uuid)
    idempotency_key: str
    timestamp: str = Field(default_factory=_utc_now)
    source: str = "cloud"
    pipeline_version: str = "1.0.0"
    schema_id: str
    trade_group_id: Optional[str] = None
    trade_id: Optional[str] = None
    payload: Dict[str, Any]


class TradeDoc(BaseModel):
    """Control document sent by producer before trade data. Triggers ingestion."""
    trade_group_id: str
    expected_count: int
    trade_ids: List[str]
    timestamp: str = Field(default_factory=_utc_now)
    source: str = "cloud"
    pipeline_version: str = "1.0.0"
    schema_id: str
