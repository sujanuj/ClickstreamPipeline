"""
producer/schemas.py — Event Schema Definition
===============================================

WHY THIS FILE EXISTS:
  Every event flowing through the pipeline — from producer to Kafka to
  storage to both the speed and batch layers — needs to agree on the same
  shape. This file is that single source of truth. If you change a field
  here, every consumer downstream needs to handle it, which is exactly the
  kind of "schema evolution" problem real data engineering teams deal with.

WHY PYDANTIC:
  Pydantic validates data at runtime and gives clear errors if a field is
  missing or the wrong type. This catches malformed events at the producer
  before they ever reach Kafka, rather than discovering the problem deep
  in a downstream consumer days later.

EVENT FIELDS, AND WHY EACH ONE EXISTS:
  event_id     — unique ID per event. Lets us deduplicate if the same event
                 is somehow processed twice (e.g. consumer crash + retry).
  event_type   — page_view, add_to_cart, or purchase. This is literally
                 what we're counting in later phases.
  user_id      — who did it. Lets us aggregate "events per user."
  session_id   — groups events into a single browsing session. Useful for
                 funnel-style analysis (view → cart → purchase).
  event_time   — WHEN THE EVENT ACTUALLY HAPPENED (from the client's
                 perspective). This is different from when it arrives at
                 our system — see below.
  ingestion_time — WHEN OUR SYSTEM RECEIVED IT. Set by the producer at send
                 time. The gap between event_time and ingestion_time is
                 what makes "late-arriving events" possible and is exactly
                 what Phase 4's reconciliation will measure.
  metadata     — a free-form dict for event-specific extras (e.g. product_id
                 for add_to_cart, amount for purchase). Keeping this generic
                 avoids needing a different schema per event type.

WHY event_time AND ingestion_time ARE BOTH NEEDED:
  Imagine a mobile app user loses signal for 2 minutes. Their clicks still
  happen (event_time keeps advancing), but they don't reach our system
  until connectivity returns (ingestion_time jumps forward all at once).
  A windowed aggregation that uses event_time would correctly bucket those
  clicks into the window they actually happened in. One that uses
  ingestion_time would incorrectly bucket them into "right now," even
  though they're 2 minutes stale. This distinction is the entire reason
  Lambda architectures exist: the speed layer typically can't wait for
  late data, so it processes by ingestion order and is slightly wrong;
  the batch layer re-reads everything by event_time later and corrects it.
"""

from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum
from pydantic import BaseModel, Field
import uuid


class EventType(str, Enum):
    PAGE_VIEW = "page_view"
    ADD_TO_CART = "add_to_cart"
    PURCHASE = "purchase"


class ClickstreamEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType
    user_id: str
    session_id: str
    event_time: datetime          # when it actually happened
    ingestion_time: datetime      # when our system received it
    metadata: Dict[str, Any] = Field(default_factory=dict)

    class Config:
        use_enum_values = True

    def to_json_bytes(self) -> bytes:
        """Serialize for sending over Kafka (Kafka messages are bytes)."""
        return self.json().encode("utf-8")

    @classmethod
    def from_json_bytes(cls, data: bytes) -> "ClickstreamEvent":
        """Deserialize a Kafka message back into an event object."""
        return cls.parse_raw(data)
