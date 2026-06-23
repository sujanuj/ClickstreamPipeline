"""
tests/test_batch_job.py — Batch Job Tests
==============================================

These tests require a real Postgres connection (the batch job's whole
purpose is running SQL aggregation against raw_events), so unlike
test_aggregator.py, these are integration tests, not pure-Python unit
tests. Run docker-compose up -d before running these.

Each test inserts known events directly into raw_events, runs the batch
job, and checks the resulting batch_counts rows — this gives us full
control over exactly which events exist, including deliberately "late"
ones, without depending on the live producer's randomness.
"""

import uuid
from datetime import datetime, timezone, timedelta
import pytest

from storage.db import get_connection, init_db
from batch_layer.batch_job import run_batch_job

TEST_USER_PREFIX = "batch-test-user"


@pytest.fixture(autouse=True)
def clean_test_data():
    """Remove any test data before AND after each test, for isolation."""
    init_db()
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM raw_events WHERE user_id LIKE %s", (f"{TEST_USER_PREFIX}%",))
            cur.execute("DELETE FROM batch_counts WHERE user_id LIKE %s", (f"{TEST_USER_PREFIX}%",))
        conn.commit()
    finally:
        conn.close()
    yield
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM raw_events WHERE user_id LIKE %s", (f"{TEST_USER_PREFIX}%",))
            cur.execute("DELETE FROM batch_counts WHERE user_id LIKE %s", (f"{TEST_USER_PREFIX}%",))
        conn.commit()
    finally:
        conn.close()


def _insert_raw_event(user_id, event_type, event_time, ingestion_time=None):
    """
    Insert a single row directly into raw_events, bypassing Kafka entirely.
    This lets tests control event_time precisely, including simulating
    events that "arrived late" without needing to run the real producer.
    """
    if ingestion_time is None:
        ingestion_time = event_time

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO raw_events (event_id, event_type, user_id, session_id,
                                         event_time, ingestion_time, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, '{}')
            """, (str(uuid.uuid4()), event_type, user_id, "test-session",
                  event_time, ingestion_time))
        conn.commit()
    finally:
        conn.close()


def _get_batch_count(user_id, event_type, window_start):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT event_count FROM batch_counts
                WHERE user_id = %s AND event_type = %s AND window_start = %s
            """, (user_id, event_type, window_start))
            row = cur.fetchone()
            return row["event_count"] if row else None
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Basic correctness
# ---------------------------------------------------------------------------

def test_batch_job_counts_events_in_a_window():
    user_id = f"{TEST_USER_PREFIX}-1"
    window = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)

    _insert_raw_event(user_id, "page_view", window + timedelta(seconds=5))
    _insert_raw_event(user_id, "page_view", window + timedelta(seconds=30))
    _insert_raw_event(user_id, "page_view", window + timedelta(seconds=55))

    run_batch_job()

    count = _get_batch_count(user_id, "page_view", window)
    assert count == 3


def test_batch_job_separates_different_event_types():
    user_id = f"{TEST_USER_PREFIX}-2"
    window = datetime(2026, 1, 1, 11, 0, 0, tzinfo=timezone.utc)

    _insert_raw_event(user_id, "page_view", window + timedelta(seconds=10))
    _insert_raw_event(user_id, "add_to_cart", window + timedelta(seconds=20))
    _insert_raw_event(user_id, "add_to_cart", window + timedelta(seconds=40))

    run_batch_job()

    assert _get_batch_count(user_id, "page_view", window) == 1
    assert _get_batch_count(user_id, "add_to_cart", window) == 2


def test_batch_job_separates_different_windows():
    user_id = f"{TEST_USER_PREFIX}-3"
    window_a = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    window_b = datetime(2026, 1, 1, 12, 1, 0, tzinfo=timezone.utc)

    _insert_raw_event(user_id, "page_view", window_a + timedelta(seconds=10))
    _insert_raw_event(user_id, "page_view", window_b + timedelta(seconds=10))
    _insert_raw_event(user_id, "page_view", window_b + timedelta(seconds=20))

    run_batch_job()

    assert _get_batch_count(user_id, "page_view", window_a) == 1
    assert _get_batch_count(user_id, "page_view", window_b) == 2


# ---------------------------------------------------------------------------
# THE key test: the batch layer correctly places late events in their
# TRUE window, unlike the speed layer which would have dropped them.
# ---------------------------------------------------------------------------

def test_batch_job_correctly_places_late_arriving_event():
    """
    Simulates exactly the scenario the whole project is built around:
    an event whose event_time is in window W, but whose ingestion_time
    (when it actually reached Kafka/raw_events) is much later — as if
    the speed layer's grace period for window W had already passed.

    The batch job doesn't care about ingestion_time at all — it groups
    strictly by event_time. So this "late" event should be correctly
    counted in window W, proving the batch layer succeeds exactly where
    the speed layer (by design) would have dropped it.
    """
    user_id = f"{TEST_USER_PREFIX}-late"
    window = datetime(2026, 1, 1, 13, 0, 0, tzinfo=timezone.utc)

    # Two on-time events
    _insert_raw_event(user_id, "purchase", window + timedelta(seconds=5))
    _insert_raw_event(user_id, "purchase", window + timedelta(seconds=10))

    # One LATE event: event_time says it belongs to this window, but
    # ingestion_time is 3 minutes later — well past any reasonable grace period
    late_event_time = window + timedelta(seconds=45)
    late_ingestion_time = window + timedelta(minutes=3)
    _insert_raw_event(user_id, "purchase", late_event_time, ingestion_time=late_ingestion_time)

    run_batch_job()

    # The batch job should count ALL THREE events in this window — including
    # the late one — because it groups by event_time, not arrival order.
    count = _get_batch_count(user_id, "purchase", window)
    assert count == 3, (
        "Batch layer should count the late event in its TRUE window "
        "(grouped by event_time), even though it arrived long after "
        "the window's nominal grace period would have closed."
    )


def test_batch_job_is_idempotent_when_run_twice():
    """
    Running the batch job twice in a row on the same data should produce
    the SAME final count, not double it. This is what makes "reprocess
    everything every time" safe to actually do on a schedule.
    """
    user_id = f"{TEST_USER_PREFIX}-idempotent"
    window = datetime(2026, 1, 1, 14, 0, 0, tzinfo=timezone.utc)

    _insert_raw_event(user_id, "page_view", window + timedelta(seconds=5))
    _insert_raw_event(user_id, "page_view", window + timedelta(seconds=10))

    run_batch_job()
    first_count = _get_batch_count(user_id, "page_view", window)

    run_batch_job()  # run again with no new data
    second_count = _get_batch_count(user_id, "page_view", window)

    assert first_count == 2
    assert second_count == 2, "Re-running the batch job should not double-count"


def test_batch_job_picks_up_new_events_on_next_run():
    """
    If new events are inserted between two runs, the second run should
    reflect the updated total — proving "reprocess everything" actually
    keeps results current, just at the cost of redoing unchanged work too.
    """
    user_id = f"{TEST_USER_PREFIX}-incremental"
    window = datetime(2026, 1, 1, 15, 0, 0, tzinfo=timezone.utc)

    _insert_raw_event(user_id, "page_view", window + timedelta(seconds=5))
    run_batch_job()
    assert _get_batch_count(user_id, "page_view", window) == 1

    _insert_raw_event(user_id, "page_view", window + timedelta(seconds=20))
    run_batch_job()
    assert _get_batch_count(user_id, "page_view", window) == 2
