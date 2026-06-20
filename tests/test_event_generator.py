"""
tests/test_event_generator.py — Event Generator and Schema Tests
====================================================================

These tests don't require Kafka or Postgres to be running — they test
the pure Python logic of event generation and serialization in isolation,
which is the right place to catch schema bugs before they ever touch
infrastructure.
"""

import json
from datetime import datetime, timedelta

from producer.schemas import ClickstreamEvent, EventType
from producer.event_generator import generate_event, LATE_EVENT_DELAY_SECONDS


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------

def test_event_has_required_fields():
    event = ClickstreamEvent(
        event_type=EventType.PAGE_VIEW,
        user_id="user-1",
        session_id="session-1",
        event_time=datetime.utcnow(),
        ingestion_time=datetime.utcnow(),
    )
    assert event.event_id is not None
    assert event.event_type == EventType.PAGE_VIEW
    assert event.user_id == "user-1"


def test_event_id_is_unique_per_instance():
    """Two separately created events should never collide on event_id."""
    e1 = ClickstreamEvent(
        event_type=EventType.PAGE_VIEW, user_id="u1", session_id="s1",
        event_time=datetime.utcnow(), ingestion_time=datetime.utcnow(),
    )
    e2 = ClickstreamEvent(
        event_type=EventType.PAGE_VIEW, user_id="u1", session_id="s1",
        event_time=datetime.utcnow(), ingestion_time=datetime.utcnow(),
    )
    assert e1.event_id != e2.event_id


def test_event_serialization_round_trip():
    """
    An event converted to JSON bytes (as sent over Kafka) and back
    should be identical to the original — this is what proves our
    Kafka producer/consumer pair won't silently corrupt data.
    """
    original = ClickstreamEvent(
        event_type=EventType.ADD_TO_CART,
        user_id="user-42",
        session_id="session-99",
        event_time=datetime.utcnow(),
        ingestion_time=datetime.utcnow(),
        metadata={"product_id": "abc-123", "price": 19.99},
    )

    serialized = original.to_json_bytes()
    restored = ClickstreamEvent.from_json_bytes(serialized)

    assert restored.event_id == original.event_id
    assert restored.event_type == original.event_type
    assert restored.user_id == original.user_id
    assert restored.metadata == original.metadata


def test_metadata_defaults_to_empty_dict():
    """Events without explicit metadata shouldn't crash or be None."""
    event = ClickstreamEvent(
        event_type=EventType.PAGE_VIEW, user_id="u1", session_id="s1",
        event_time=datetime.utcnow(), ingestion_time=datetime.utcnow(),
    )
    assert event.metadata == {}


# ---------------------------------------------------------------------------
# Event generator tests
# ---------------------------------------------------------------------------

def test_generate_event_produces_valid_event():
    event = generate_event(user_id="u1", session_id="s1")
    assert event.user_id == "u1"
    assert event.session_id == "s1"
    assert event.event_type in [EventType.PAGE_VIEW, EventType.ADD_TO_CART, EventType.PURCHASE]


def test_generate_event_metadata_matches_type():
    """add_to_cart events should have product_id; purchase events should have order_id."""
    # Generate many events and check the ones of each type have correct metadata.
    # (Probabilistic — but with enough samples we'll hit every type.)
    found_cart = False
    found_purchase = False

    for _ in range(200):
        event = generate_event(user_id="u1", session_id="s1")
        if event.event_type == EventType.ADD_TO_CART:
            assert "product_id" in event.metadata
            found_cart = True
        elif event.event_type == EventType.PURCHASE:
            assert "order_id" in event.metadata
            found_purchase = True

    assert found_cart, "Expected at least one add_to_cart event in 200 samples"
    assert found_purchase, "Expected at least one purchase event in 200 samples"


def test_late_events_have_event_time_before_ingestion_time():
    """
    THE key test for this phase — proves our "late event" mechanism works.
    A late event's event_time should be meaningfully earlier than its
    ingestion_time, simulating a client that buffered offline.

    We generate many events and assert that SOME of them show this gap,
    since lateness is probabilistic (LATE_EVENT_PROBABILITY).
    """
    found_late_event = False

    for _ in range(500):  # enough samples to reliably hit a low-probability case
        event = generate_event(user_id="u1", session_id="s1")
        gap_seconds = (event.ingestion_time - event.event_time).total_seconds()

        if gap_seconds > 5:  # meaningfully late, not just clock jitter
            found_late_event = True
            # The gap should roughly match our configured delay
            assert abs(gap_seconds - LATE_EVENT_DELAY_SECONDS) < 5

    assert found_late_event, (
        "Expected at least one late-arriving event in 500 samples. "
        "This is the core mechanism the whole Lambda architecture project "
        "depends on — if this fails, check LATE_EVENT_PROBABILITY in .env"
    )


def test_normal_events_have_matching_event_and_ingestion_time():
    """Non-late events should have event_time == ingestion_time (or very close)."""
    found_normal_event = False

    for _ in range(50):
        event = generate_event(user_id="u1", session_id="s1")
        gap_seconds = (event.ingestion_time - event.event_time).total_seconds()
        if gap_seconds < 1:
            found_normal_event = True

    assert found_normal_event, "Expected at least one non-late event in 50 samples"
