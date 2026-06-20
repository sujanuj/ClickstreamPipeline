"""
speed_layer/aggregator.py — Real-Time Windowed Aggregation
=============================================================

WHY THIS FILE EXISTS:
  This is the "speed layer" of the Lambda architecture. It reads the same
  Kafka topic that raw_event_writer.py reads (Phase 1), but instead of
  filing events away untouched, it counts them in real time, bucketed into
  one-minute windows, and writes finalized counts to Postgres as soon as
  it's confident a window won't receive any more events.

  Critically, this consumer is in a DIFFERENT Kafka consumer group than
  raw_event_writer.py ("speed-layer-aggregator" vs "raw-storage-writer").
  Because Kafka treats different consumer groups as independent readers,
  both consumers receive every single message — this consumer doesn't
  "compete" with the raw storage writer for events. This is the mechanical
  fact that makes Lambda architecture possible: one stream, multiple
  independent interpretations of it.

THE WINDOWING MODEL:
  Time is divided into fixed one-minute buckets based on event_time (NOT
  ingestion_time — see schemas.py for why this distinction exists).

    window_start = floor(event_time to the nearest minute)
    window_end   = window_start + 1 minute

  Example: an event with event_time=2:00:47 PM belongs to the window
  [2:00:00, 2:01:00).

THE GRACE PERIOD — handling late events without waiting forever:
  We can't keep every window open indefinitely waiting for stragglers —
  that would make the "speed" layer not fast. Instead, each window stays
  open in memory for GRACE_PERIOD_SECONDS after it would have closed.
  Once that grace period passes, we finalize the window: write whatever
  count we have to Postgres, and remove it from memory.

  If an event arrives whose window has ALREADY been finalized and removed
  from memory, we can't un-finalize it. We log it as a "late/dropped" event
  and increment a separate dropped-events counter — this is the visible,
  honest admission that the speed layer is approximately right, not
  perfectly right. Phase 3's batch layer is what corrects this, by
  re-reading raw_events (which still has every event, since Phase 1's
  writer never drops anything).

WHY IN-MEMORY STATE, AND WHAT HAPPENS IF THIS PROCESS CRASHES:
  Keeping open windows in a Python dict (not in Postgres) is what makes
  incrementing counts fast — no database round-trip per event. The
  tradeoff: if this process crashes, any not-yet-finalized windows in
  memory are lost. This is a real, known limitation of this simplified
  speed layer (production systems like Flink solve this with periodic
  "checkpointing" of in-memory state) — worth being upfront about in an
  interview rather than pretending it isn't a gap.

USAGE:
  python -m speed_layer.aggregator
"""

import os
import time
import threading
from datetime import datetime, timezone, timedelta
from collections import defaultdict

from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError

from storage.db import get_connection, init_db
from producer.schemas import ClickstreamEvent

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "clickstream-events")

WINDOW_SECONDS       = 60   # one-minute buckets
GRACE_PERIOD_SECONDS = 30   # how long a window stays open after it "ends"
FINALIZE_CHECK_INTERVAL = 5  # how often we check for windows ready to finalize


def _window_start(event_time: datetime) -> datetime:
    """
    Round event_time DOWN to the start of its one-minute window.

    Example: 2:00:47.123 PM -> 2:00:00.000 PM
    This is the same "floor division" idea used in fixed_window.py from
    the RateLimiter project, just applied to wall-clock minutes instead
    of arbitrary window_seconds.
    """
    epoch_seconds = event_time.timestamp()
    window_epoch = (epoch_seconds // WINDOW_SECONDS) * WINDOW_SECONDS
    return datetime.fromtimestamp(window_epoch, tz=timezone.utc)


UPSERT_SPEED_COUNT_SQL = """
INSERT INTO speed_counts (user_id, event_type, window_start, window_end,
                            event_count, late_events_dropped)
VALUES (%(user_id)s, %(event_type)s, %(window_start)s, %(window_end)s,
        %(event_count)s, %(late_events_dropped)s)
ON CONFLICT (user_id, event_type, window_start)
DO UPDATE SET
    event_count = speed_counts.event_count + EXCLUDED.event_count,
    late_events_dropped = speed_counts.late_events_dropped + EXCLUDED.late_events_dropped
"""


class WindowedAggregator:
    """
    Holds in-memory counts for all currently-open windows, and periodically
    finalizes (writes to Postgres + removes from memory) windows whose
    grace period has expired.
    """

    def __init__(self):
        self.windows = defaultdict(lambda: defaultdict(int))
        self.late_drops = defaultdict(lambda: defaultdict(int))
        self.lock = threading.Lock()
        self.finalized_windows = set()

    def add_event(self, event: ClickstreamEvent):
        """
        Increment the count for this event's window. If the window has
        already been finalized (its grace period passed), record this as
        a dropped late event instead of silently losing it.
        """
        w_start = _window_start(event.event_time)
        key = (event.user_id, event.event_type)

        with self.lock:
            if w_start in self.finalized_windows:
                self.late_drops[w_start][key] += 1
                print(f"   late event dropped: user={event.user_id} "
                      f"type={event.event_type} window={w_start.strftime('%H:%M:%S')}")
            else:
                self.windows[w_start][key] += 1

    def finalize_ready_windows(self):
        """
        Check every currently-open window. If its grace period has passed,
        write its counts to Postgres and remove it from memory.
        """
        now = datetime.now(timezone.utc)
        ready_to_finalize = []

        with self.lock:
            for w_start in list(self.windows.keys()):
                window_end = w_start + timedelta(seconds=WINDOW_SECONDS)
                grace_deadline = window_end + timedelta(seconds=GRACE_PERIOD_SECONDS)
                if now >= grace_deadline:
                    ready_to_finalize.append(w_start)

            if not ready_to_finalize:
                return

            conn = get_connection()
            try:
                with conn.cursor() as cur:
                    for w_start in ready_to_finalize:
                        window_end = w_start + timedelta(seconds=WINDOW_SECONDS)
                        counts = self.windows.pop(w_start)
                        drops = self.late_drops.pop(w_start, {})

                        for (user_id, event_type), count in counts.items():
                            dropped = drops.get((user_id, event_type), 0)
                            cur.execute(UPSERT_SPEED_COUNT_SQL, {
                                "user_id": user_id,
                                "event_type": event_type,
                                "window_start": w_start,
                                "window_end": window_end,
                                "event_count": count,
                                "late_events_dropped": dropped,
                            })

                        self.finalized_windows.add(w_start)
                conn.commit()
                print(f"   finalized window {[w.strftime('%H:%M:%S') for w in ready_to_finalize]}")
            finally:
                conn.close()

            cutoff = now - timedelta(hours=1)
            self.finalized_windows = {
                w for w in self.finalized_windows if w > cutoff
            }


def _finalizer_loop(aggregator: WindowedAggregator, stop_event: threading.Event):
    """Background thread: periodically check for windows ready to finalize."""
    while not stop_event.is_set():
        aggregator.finalize_ready_windows()
        stop_event.wait(FINALIZE_CHECK_INTERVAL)


def run_aggregator():
    init_db()

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "speed-layer-aggregator",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([KAFKA_TOPIC])

    aggregator = WindowedAggregator()
    stop_event = threading.Event()
    finalizer_thread = threading.Thread(
        target=_finalizer_loop, args=(aggregator, stop_event), daemon=True
    )
    finalizer_thread.start()

    processed_count = 0
    print(f"Speed layer consuming from '{KAFKA_TOPIC}' "
          f"(window={WINDOW_SECONDS}s, grace={GRACE_PERIOD_SECONDS}s)...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Kafka error: {msg.error()}")
                continue

            try:
                event = ClickstreamEvent.from_json_bytes(msg.value())
                aggregator.add_event(event)
                processed_count += 1

                if processed_count % 50 == 0:
                    print(f"   processed={processed_count}  "
                          f"open_windows={len(aggregator.windows)}")

            except Exception as e:
                print(f"Failed to process message: {e}")
                continue

    except KeyboardInterrupt:
        print(f"\nStopping. Processed: {processed_count}")
        stop_event.set()
        finalizer_thread.join(timeout=5)
        aggregator.finalize_ready_windows()
    finally:
        consumer.close()


if __name__ == "__main__":
    run_aggregator()
