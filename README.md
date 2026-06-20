# Clickstream Lambda Pipeline

A streaming + batch data pipeline implementing Lambda Architecture: the same e-commerce clickstream events flow through both a real-time speed layer and a scheduled batch layer, landing in the same store so their outputs can be reconciled against each other.

## Why Lambda Architecture

A pure streaming pipeline is fast but can be wrong — events that arrive late (a mobile client buffering offline, then reconnecting) get counted in whatever time window they *arrive* in, not the window they actually *happened* in. A pure batch pipeline is correct but slow — you wait for a full data dump before computing anything.

Lambda architecture runs both: a speed layer for low-latency approximate results, and a batch layer that periodically reprocesses everything from the permanent raw log to produce the correct numbers. This project deliberately simulates late-arriving events so the discrepancy between the two layers — and the correction — is visible and explainable, not theoretical.

## Project phases

| Phase | What | Status |
|-------|------|--------|
| 1 | Event producer + Kafka + raw event storage | ✅ this phase |
| 2 | Speed layer — windowed aggregation consumer | 🔜 next |
| 3 | Batch layer — scheduled reprocessing job | 🔜 |
| 4 | Reconciliation — compare and correct speed vs batch | 🔜 |

## Architecture (Phase 1)

```
producer/event_generator.py
        │  publishes ClickstreamEvent (some deliberately late)
        ▼
   Kafka topic: clickstream-events
        │
        ▼
storage/raw_event_writer.py  (consumer group: raw-storage-writer)
        │  writes every event, unaggregated, idempotently
        ▼
   Postgres: raw_events table
```

## Project structure

```
producer/
  schemas.py           ClickstreamEvent definition (event_time vs ingestion_time)
  event_generator.py   Simulates realistic traffic, including late arrivals
storage/
  db.py                Postgres connection + table schema
  raw_event_writer.py  Kafka consumer → Postgres (idempotent, dumb on purpose)
tests/
  test_event_generator.py
```

## The key design decision: event_time vs ingestion_time

Every event has two timestamps:
- **event_time** — when it actually happened, from the client's perspective
- **ingestion_time** — when our system received it

For most events these are the same moment. But the producer deliberately makes a small percentage of events "late": event_time is set in the past, while ingestion_time is now. This single mechanic is what creates the entire reason this project exists — a speed layer processing by arrival order will misplace late events into the wrong time window, while a batch layer re-reading by event_time later will get it right.

## Running it

```bash
# start kafka, zookeeper, postgres
docker-compose up -d

# environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# initialize the database schema
python -m storage.db

# terminal 1: start the raw event writer (consumer)
python -m storage.raw_event_writer

# terminal 2: start the producer
python -m producer.event_generator
```

Watch both terminals — the producer logs sent/late counts, the writer logs written/duplicate counts. After a minute, check Postgres:

```bash
docker exec -it pipeline-postgres psql -U pipeline -d clickstream -c "SELECT event_type, count(*) FROM raw_events GROUP BY event_type;"
```

## Running the tests

```bash
pytest tests/ -v
```

These test the event schema and generator logic in isolation — no Kafka or Postgres required. The key test, `test_late_events_have_event_time_before_ingestion_time`, proves the late-arrival mechanism actually works, since everything in Phases 2-4 depends on it.

## Tech stack

Python 3.9+, Kafka (Confluent images via Docker), Postgres 16, confluent-kafka, psycopg2, Pydantic, Faker
