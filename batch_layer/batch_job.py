"""
batch_layer/batch_job.py — Batch Reprocessing Job
=====================================================

WHY THIS FILE EXISTS:
  This is the "batch layer" of the Lambda architecture. While the speed
  layer (Phase 2) counts events live and has to make irreversible decisions
  about late arrivals, this job has no such pressure: it reads directly
  from raw_events — the permanent, complete log where Phase 1's writer
  filed every event, including every late one — and computes the TRUE
  count for each window with a single SQL aggregation query.

  There's no streaming, no in-memory state, no grace period, no dropped
  events here. That's the whole point of a batch layer: trade latency
  (it only runs when invoked, not continuously) for correctness (it sees
  all the data before computing anything).

WHY THIS RUNS "REPROCESS EVERYTHING EVERY TIME":
  Rather than tracking a watermark of "what have I already processed,"
  this job recomputes every window from scratch from the full contents of
  raw_events, every time it runs. This is the simplest possible correct
  implementation — there's no bookkeeping that could itself have a bug,
  and no risk of silently missing a window due to an off-by-one in
  watermark tracking. The cost is wasted recomputation of windows that
  haven't changed — a real, known tradeoff. A production system would
  likely track a watermark and only reprocess new/changed windows, but
  starting with full correctness and identifying the optimization
  afterward is the right order of operations.

HOW THE SQL DOES WHAT THE SPEED LAYER DOES IN PYTHON:
  Phase 2's aggregator.py manually buckets events into windows using
  Python dictionaries and floor-division on timestamps. Here, Postgres's
  date_trunc('minute', event_time) does the exact same bucketing, but as
  a single SQL expression evaluated by the database engine — because all
  the data already exists in one table, there's no need to maintain
  in-memory state across a stream of incoming messages.

  date_trunc('minute', event_time) rounds event_time DOWN to the start
  of its containing minute — exactly the same operation as aggregator.py's
  _window_start() function, just expressed in SQL instead of Python.

THE KEY PROOF THIS PHASE PRODUCES:
  Because this job groups by event_time (the TRUE time something happened),
  not ingestion_time or arrival order, a late event that Phase 2 dropped
  from its window will correctly appear in THIS job's count for that
  window. Phase 4 will show this directly: a window where
  speed_counts.event_count < batch_counts.event_count is a window where a
  late arrival was missed by the fast path and only caught by the batch path.

USAGE:
  python -m batch_layer.batch_job
  Runs once and exits — this is meant to be invoked on a schedule (cron,
  Airflow, etc.), not left running like the producer/consumers.
"""

from datetime import datetime, timezone

from storage.db import get_connection, init_db


BATCH_AGGREGATION_SQL = """
SELECT
    user_id,
    event_type,
    date_trunc('minute', event_time) AS window_start,
    date_trunc('minute', event_time) + interval '1 minute' AS window_end,
    count(*) AS event_count
FROM raw_events
GROUP BY user_id, event_type, date_trunc('minute', event_time)
ORDER BY window_start
"""

UPSERT_BATCH_COUNT_SQL = """
INSERT INTO batch_counts (user_id, event_type, window_start, window_end, event_count)
VALUES (%(user_id)s, %(event_type)s, %(window_start)s, %(window_end)s, %(event_count)s)
ON CONFLICT (user_id, event_type, window_start)
DO UPDATE SET
    event_count = EXCLUDED.event_count,
    computed_at = now()
"""


def run_batch_job() -> dict:
    """
    Run one full batch reprocessing pass: read all of raw_events, compute
    true per-window counts, and upsert them into batch_counts.

    Returns a small summary dict — useful for logging and for tests.
    """
    init_db()
    conn = get_connection()

    windows_written = 0
    total_events_processed = 0

    try:
        with conn.cursor() as cur:
            cur.execute(BATCH_AGGREGATION_SQL)
            rows = cur.fetchall()

            for row in rows:
                cur.execute(UPSERT_BATCH_COUNT_SQL, {
                    "user_id": row["user_id"],
                    "event_type": row["event_type"],
                    "window_start": row["window_start"],
                    "window_end": row["window_end"],
                    "event_count": row["event_count"],
                })
                windows_written += 1
                total_events_processed += row["event_count"]

        conn.commit()
    finally:
        conn.close()

    summary = {
        "windows_written": windows_written,
        "total_events_processed": total_events_processed,
        "run_at": datetime.now(timezone.utc).isoformat(),
    }
    print(f"Batch job complete: {windows_written} windows, "
          f"{total_events_processed} total events processed")
    return summary


if __name__ == "__main__":
    run_batch_job()
