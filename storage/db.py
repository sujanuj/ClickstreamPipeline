"""
storage/db.py — Postgres Connection and Schema Setup
======================================================

WHY THIS FILE EXISTS:
  Centralizes how every part of the pipeline talks to Postgres, and defines
  the tables that the rest of the project depends on. Getting this table
  design right in Phase 1 means Phases 2-4 just add new tables without
  ever needing to touch this one.

TABLE DESIGN, AND WHY:

  raw_events
    This is the permanent, append-only log of every event that has ever
    been seen, written here by the Phase 1 consumer (raw_event_writer.py).
    Both the batch layer (Phase 3) and the reconciliation step (Phase 4)
    read from this table — it's the "ground truth" that nothing else is
    allowed to be more authoritative than.

    Columns mirror the ClickstreamEvent schema exactly, plus an auto
    `received_at` timestamp set by Postgres itself (DEFAULT now()) as a
    sanity-check audit column, separate from the application-level
    ingestion_time.

  Why we don't aggregate here:
    This table is intentionally "dumb" — one row per event, no counting,
    no grouping. Aggregation happens in Phase 2 (speed_counts) and Phase 3
    (batch_counts), which are separate tables. Keeping raw storage
    completely unaggregated means we can always recompute any aggregate
    later, from scratch, exactly the property a "batch layer" needs.

  Indexing choices:
    - event_time is indexed because every aggregation query downstream
      groups by time windows over this column.
    - user_id is indexed because per-user metrics are a common query shape.
    - event_id has a UNIQUE constraint, which gives us free deduplication:
      if the consumer crashes and reprocesses a message it already wrote,
      the INSERT will conflict and we can safely ignore it (see
      raw_event_writer.py for how this is used).
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", 5432),
    "dbname": os.getenv("POSTGRES_DB", "clickstream"),
    "user": os.getenv("POSTGRES_USER", "pipeline"),
    "password": os.getenv("POSTGRES_PASSWORD", "pipeline"),
}


def get_connection():
    """
    Open a new Postgres connection.

    We use a fresh connection per call rather than a long-lived pool here
    because this project's consumers are single-threaded scripts, not a
    high-concurrency web server — simplicity over connection pooling
    machinery we don't need yet.
    """
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)


CREATE_RAW_EVENTS_TABLE = """
CREATE TABLE IF NOT EXISTS raw_events (
    id              BIGSERIAL PRIMARY KEY,
    event_id        UUID UNIQUE NOT NULL,
    event_type      TEXT NOT NULL,
    user_id         TEXT NOT NULL,
    session_id      TEXT NOT NULL,
    event_time      TIMESTAMPTZ NOT NULL,
    ingestion_time  TIMESTAMPTZ NOT NULL,
    metadata        JSONB NOT NULL DEFAULT '{}',
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_raw_events_event_time ON raw_events (event_time);
CREATE INDEX IF NOT EXISTS idx_raw_events_user_id    ON raw_events (user_id);
CREATE INDEX IF NOT EXISTS idx_raw_events_event_type ON raw_events (event_type);
"""


def init_db():
    """
    Create all tables if they don't already exist. Safe to run repeatedly —
    CREATE TABLE IF NOT EXISTS means re-running this won't wipe data.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_RAW_EVENTS_TABLE)
        conn.commit()
        print("✅ Database schema ready (raw_events table)")
    finally:
        conn.close()


if __name__ == "__main__":
    init_db()
