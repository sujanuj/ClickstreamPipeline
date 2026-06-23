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

CREATE_SPEED_COUNTS_TABLE = """
CREATE TABLE IF NOT EXISTS speed_counts (
    id                   BIGSERIAL PRIMARY KEY,
    user_id              TEXT NOT NULL,
    event_type           TEXT NOT NULL,
    window_start         TIMESTAMPTZ NOT NULL,
    window_end           TIMESTAMPTZ NOT NULL,
    event_count          INTEGER NOT NULL DEFAULT 0,
    late_events_dropped  INTEGER NOT NULL DEFAULT 0,
    finalized_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, event_type, window_start)
);

CREATE INDEX IF NOT EXISTS idx_speed_counts_window ON speed_counts (window_start);
"""

CREATE_BATCH_COUNTS_TABLE = """
CREATE TABLE IF NOT EXISTS batch_counts (
    id                   BIGSERIAL PRIMARY KEY,
    user_id              TEXT NOT NULL,
    event_type           TEXT NOT NULL,
    window_start         TIMESTAMPTZ NOT NULL,
    window_end           TIMESTAMPTZ NOT NULL,
    event_count          INTEGER NOT NULL DEFAULT 0,
    computed_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, event_type, window_start)
);

CREATE INDEX IF NOT EXISTS idx_batch_counts_window ON batch_counts (window_start);
"""


def init_db():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_RAW_EVENTS_TABLE)
            cur.execute(CREATE_SPEED_COUNTS_TABLE)
            cur.execute(CREATE_BATCH_COUNTS_TABLE)
        conn.commit()
        print("✅ Database schema ready (raw_events, speed_counts, batch_counts tables)")
    finally:
        conn.close()


if __name__ == "__main__":
    init_db()
