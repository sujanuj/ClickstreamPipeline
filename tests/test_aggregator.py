"""
tests/test_aggregator.py — Windowed Aggregator Tests
=========================================================

These tests exercise the WindowedAggregator class directly, in memory,
without needing Kafka or Postgres running — we only need Postgres for
the actual finalize_ready_windows() write, which we test separately with
a real (test) database connection.
"""

from datetime import datetime, timezone, timedelta
import pytest

from speed_layer.aggregator import (
    WindowedAggregator, _window_start, WINDOW_SECONDS, GRACE_PERIOD_SECONDS
)
from producer.schemas import ClickstreamEvent, EventType


def _make_event(event_time: datetime, user_id="u1", event_type=EventType.PAGE_VIEW):
    return ClickstreamEvent(
        event_type=event_type,
        user_id=user_id,
        session_id="s1",
        event_time=event_time,
        ingestion_time=event_time,  # not relevant for these tests
    )


# ---------------------------------------------------------------------------
# Window bucketing tests
# ---------------------------------------------------------------------------

def test_window_start_rounds_down_to_the_minute():
    t = datetime(2026, 1, 1, 14, 0, 47, tzinfo=timezone.utc)  # 2:00:47 PM
    bucket = _window_start(t)
    assert bucket == datetime(2026, 1, 1, 14, 0, 0, tzinfo=timezone.utc)


def test_events_in_same_minute_share_a_window():
    t1 = datetime(2026, 1, 1, 14, 0, 5, tzinfo=timezone.utc)
    t2 = datetime(2026, 1, 1, 14, 0, 55, tzinfo=timezone.utc)
    assert _window_start(t1) == _window_start(t2)


def test_events_in_different_minutes_get_different_windows():
    t1 = datetime(2026, 1, 1, 14, 0, 59, tzinfo=timezone.utc)
    t2 = datetime(2026, 1, 1, 14, 1, 1, tzinfo=timezone.utc)
    assert _window_start(t1) != _window_start(t2)


# ---------------------------------------------------------------------------
# Aggregator in-memory counting tests
# ---------------------------------------------------------------------------

def test_add_event_increments_window_count():
    agg = WindowedAggregator()
    t = datetime(2026, 1, 1, 14, 0, 0, tzinfo=timezone.utc)

    agg.add_event(_make_event(t, user_id="alice", event_type=EventType.PAGE_VIEW))
    agg.add_event(_make_event(t, user_id="alice", event_type=EventType.PAGE_VIEW))
    agg.add_event(_make_event(t, user_id="alice", event_type=EventType.PAGE_VIEW))

    w_start = _window_start(t)
    assert agg.windows[w_start][("alice", EventType.PAGE_VIEW)] == 3


def test_different_users_counted_separately():
    agg = WindowedAggregator()
    t = datetime(2026, 1, 1, 14, 0, 0, tzinfo=timezone.utc)

    agg.add_event(_make_event(t, user_id="alice"))
    agg.add_event(_make_event(t, user_id="alice"))
    agg.add_event(_make_event(t, user_id="bob"))

    w_start = _window_start(t)
    assert agg.windows[w_start][("alice", EventType.PAGE_VIEW)] == 2
    assert agg.windows[w_start][("bob", EventType.PAGE_VIEW)] == 1


def test_different_event_types_counted_separately():
    agg = WindowedAggregator()
    t = datetime(2026, 1, 1, 14, 0, 0, tzinfo=timezone.utc)

    agg.add_event(_make_event(t, event_type=EventType.PAGE_VIEW))
    agg.add_event(_make_event(t, event_type=EventType.ADD_TO_CART))
    agg.add_event(_make_event(t, event_type=EventType.ADD_TO_CART))

    w_start = _window_start(t)
    assert agg.windows[w_start][("u1", EventType.PAGE_VIEW)] == 1
    assert agg.windows[w_start][("u1", EventType.ADD_TO_CART)] == 2


# ---------------------------------------------------------------------------
# The key test: late events are dropped (not counted) once a window
# has been finalized — this is the speed layer's defining limitation.
# ---------------------------------------------------------------------------

def test_event_after_finalization_is_dropped_not_counted():
    """
    Simulates a window already being finalized (as if its grace period
    had passed), then an event arrives late for that same window.
    It should be recorded as a "late drop," NOT added to window counts.
    """
    agg = WindowedAggregator()
    t = datetime(2026, 1, 1, 14, 0, 0, tzinfo=timezone.utc)
    w_start = _window_start(t)

    # Manually mark this window as already finalized, simulating what
    # finalize_ready_windows() would have done after the grace period passed
    agg.finalized_windows.add(w_start)

    late_event = _make_event(t, user_id="alice", event_type=EventType.PURCHASE)
    agg.add_event(late_event)

    # Should NOT appear in active window counts...
    assert ("alice", EventType.PURCHASE) not in agg.windows[w_start]
    # ...but SHOULD be recorded as a dropped late event
    assert agg.late_drops[w_start][("alice", EventType.PURCHASE)] == 1


def test_on_time_event_for_open_window_is_not_dropped():
    """Sanity check: events for windows that are NOT yet finalized count normally."""
    agg = WindowedAggregator()
    t = datetime(2026, 1, 1, 14, 0, 0, tzinfo=timezone.utc)
    w_start = _window_start(t)

    agg.add_event(_make_event(t, user_id="alice"))

    assert agg.windows[w_start][("alice", EventType.PAGE_VIEW)] == 1
    assert w_start not in agg.late_drops or len(agg.late_drops[w_start]) == 0


# ---------------------------------------------------------------------------
# Grace period timing test
# ---------------------------------------------------------------------------

def test_grace_period_constants_are_sane():
    """
    Quick sanity check on the configured constants — if someone changes
    these in aggregator.py, this test documents what's expected to remain
    true: windows should be at least as long as a few seconds, and grace
    periods should be a meaningful fraction of the window, not zero or
    enormous.
    """
    assert WINDOW_SECONDS >= 10
    assert GRACE_PERIOD_SECONDS > 0
    assert GRACE_PERIOD_SECONDS <= WINDOW_SECONDS * 2
