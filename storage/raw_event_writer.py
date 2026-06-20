"""
storage/raw_event_writer.py — Raw Event Persistence Consumer
===============================================================

WHY THIS FILE EXISTS:
  Kafka topics are not permanent storage — by default, messages expire
  after a retention period (often 7 days) and get deleted. If we want a
  permanent, replayable log of every event (which the batch layer in
  Phase 3 absolutely needs), something has to read every message off
  Kafka and write it somewhere durable. That's this file's only job.

  This consumer does NOT aggregate, count, or transform anything. It is
  intentionally the "dumbest" consumer in the whole pipeline — read a
  message, write it to raw_events, move on. Keeping it dumb means it's
  also the most reliable piece: there's very little that can go wrong in
  "copy this message to a database row."

CONSUMER GROUPS (an important Kafka concept):
  Kafka lets multiple consumers read the SAME topic independently, as long
  as they're in different "consumer groups." This file's consumer is in
  group "raw-storage-writer". In Phase 2, the speed layer will be in a
  DIFFERENT group, like "speed-layer-aggregator". Because they're
  different groups, both consumers receive every message — Kafka doesn't
  know or care that they're both reading the same events for different
  purposes. This is the mechanical foundation that makes Lambda
  architecture possible: one topic, multiple independent readers.

AT-LEAST-ONCE DELIVERY AND IDEMPOTENCY:
  Kafka consumers commit their "offset" (position in the topic) after
  processing a message. If the consumer crashes AFTER writing to Postgres
  but BEFORE committing the offset, it will re-read and re-process that
  same message when it restarts. This means we can receive the same
  event_id twice.

  Rather than trying to prevent this (which is hard — you'd need
  distributed transactions across Kafka and Postgres), we make the write
  IDEMPOTENT instead: the event_id UNIQUE constraint on raw_events means
  a duplicate INSERT simply fails harmlessly, and we catch that specific
  case and move on. This "design for retries instead of preventing them"
  approach is the standard real-world way to handle at-least-once
  delivery, and it's worth being able to explain in an interview.

USAGE:
  python -m storage.raw_event_writer
  Runs indefinitely, consuming from KAFKA_TOPIC and writing every event
  to the raw_events table, until you Ctrl+C it.
"""

import os
import json
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError
import psycopg2

from storage.db import get_connection, init_db
from producer.schemas import ClickstreamEvent

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "clickstream-events")

INSERT_EVENT_SQL = """
INSERT INTO raw_events (event_id, event_type, user_id, session_id,
                         event_time, ingestion_time, metadata)
VALUES (%(event_id)s, %(event_type)s, %(user_id)s, %(session_id)s,
        %(event_time)s, %(ingestion_time)s, %(metadata)s)
ON CONFLICT (event_id) DO NOTHING
"""
# ON CONFLICT DO NOTHING is the idempotency mechanism described above:
# if this event_id already exists (because we're reprocessing after a
# crash), Postgres just skips the insert instead of raising an error.


def _write_event(conn, event: ClickstreamEvent) -> bool:
    """
    Write one event to raw_events. Returns True if it was a new row,
    False if it was a duplicate (already existed).
    """
    with conn.cursor() as cur:
        cur.execute(INSERT_EVENT_SQL, {
            "event_id": event.event_id,
            "event_type": event.event_type,
            "user_id": event.user_id,
            "session_id": event.session_id,
            "event_time": event.event_time,
            "ingestion_time": event.ingestion_time,
            "metadata": json.dumps(event.metadata),
        })
        was_inserted = cur.rowcount > 0
    conn.commit()
    return was_inserted


def run_writer():
    """
    Main loop: consume from Kafka, write each message to Postgres.

    group.id="raw-storage-writer" — this consumer group name matters.
    In Phase 2, the speed layer consumer will use a DIFFERENT group.id,
    which is what lets both consumers read every message independently.
    """
    init_db()

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "raw-storage-writer",
        "auto.offset.reset": "earliest",  # on first run, read from the start
    })
    consumer.subscribe([KAFKA_TOPIC])

    conn = get_connection()

    written_count = 0
    duplicate_count = 0

    print(f"📥 Writing raw events from '{KAFKA_TOPIC}' to Postgres...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"❌ Kafka error: {msg.error()}")
                continue

            try:
                event = ClickstreamEvent.from_json_bytes(msg.value())
                was_new = _write_event(conn, event)

                if was_new:
                    written_count += 1
                else:
                    duplicate_count += 1

                if (written_count + duplicate_count) % 50 == 0:
                    print(f"   written={written_count}  duplicates_skipped={duplicate_count}")

            except Exception as e:
                print(f"❌ Failed to process message: {e}")
                # We deliberately don't crash the whole consumer on one bad
                # message — log it and keep going. A production system
                # might route this to a dead-letter topic instead.
                continue

    except KeyboardInterrupt:
        print(f"\n🛑 Stopping. Written: {written_count}  Duplicates skipped: {duplicate_count}")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    run_writer()
