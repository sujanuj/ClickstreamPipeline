"""
producer/event_generator.py — Clickstream Event Producer
===========================================================

WHY THIS FILE EXISTS:
  This is our stand-in for "real users browsing a website." Since we don't
  have actual production traffic, we simulate it — but we simulate it
  realistically enough to exercise the exact problem this whole project
  is about: events that arrive late.

HOW NORMAL EVENTS WORK:
  Most events are generated and sent immediately. event_time and
  ingestion_time are essentially the same moment — this is the common case.

HOW LATE EVENTS WORK (the important part):
  With some probability (LATE_EVENT_PROBABILITY), instead of sending an
  event for "now," we generate an event whose event_time is in the past
  (LATE_EVENT_DELAY_SECONDS ago) but whose ingestion_time is now.

  This simulates a real scenario: a mobile user's app buffers clicks while
  they have no signal, then flushes the buffer once they reconnect. From
  Kafka's perspective, this event arrives now. From the business's
  perspective, it happened minutes ago.

  This single mechanic — sending an old event_time with a current
  ingestion_time — is what creates the gap between what the speed layer
  sees (counts based on arrival) and what's actually true (counts based
  on when things really happened). Phase 2 and Phase 3 will process the
  same events differently because of this, and Phase 4 will show the
  discrepancy and correct it.

SESSION SIMULATION:
  Real users don't generate one event and disappear — they browse for a
  while (a "session"), generating a few page views, maybe an add-to-cart,
  occasionally a purchase. We model this with a tiny weighted state
  sequence rather than pure randomness, so the data has a believable shape
  (way more page_views than purchases, which is realistic).

USAGE:
  python -m producer.event_generator
  Runs indefinitely, publishing to the Kafka topic at roughly
  EVENTS_PER_SECOND, until you Ctrl+C it.
"""

import os
import time
import random
import json
from datetime import datetime, timedelta

from dotenv import load_dotenv
from confluent_kafka import Producer
from faker import Faker

from producer.schemas import ClickstreamEvent, EventType

load_dotenv()
fake = Faker()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "clickstream-events")
EVENTS_PER_SECOND       = float(os.getenv("EVENTS_PER_SECOND", 5))
LATE_EVENT_PROBABILITY  = float(os.getenv("LATE_EVENT_PROBABILITY", 0.05))
LATE_EVENT_DELAY_SECONDS = int(os.getenv("LATE_EVENT_DELAY_SECONDS", 120))

# Roughly realistic distribution: most traffic is just browsing,
# a smaller fraction adds to cart, and only a few actually purchase.
EVENT_TYPE_WEIGHTS = {
    EventType.PAGE_VIEW: 0.75,
    EventType.ADD_TO_CART: 0.18,
    EventType.PURCHASE: 0.07,
}


def _delivery_report(err, msg):
    """Kafka calls this after attempting to send each message."""
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    # Successful deliveries are silent — too noisy to print every one


def _weighted_event_type() -> EventType:
    types = list(EVENT_TYPE_WEIGHTS.keys())
    weights = list(EVENT_TYPE_WEIGHTS.values())
    return random.choices(types, weights=weights, k=1)[0]


def _build_metadata(event_type: EventType) -> dict:
    """Different event types carry different extra context."""
    if event_type == EventType.ADD_TO_CART:
        return {"product_id": fake.uuid4(), "price": round(random.uniform(5, 200), 2)}
    if event_type == EventType.PURCHASE:
        return {"order_id": fake.uuid4(), "amount": round(random.uniform(20, 500), 2)}
    return {"page": random.choice(["/home", "/product", "/search", "/cart", "/checkout"])}


def generate_event(user_id: str, session_id: str) -> ClickstreamEvent:
    """
    Build one event. Most of the time it's a "live" event (event_time ==
    now). With LATE_EVENT_PROBABILITY chance, it's a late-arriving event
    (event_time in the past, ingestion_time is now) — simulating a client
    that buffered offline and just reconnected.
    """
    now = datetime.utcnow()
    event_type = _weighted_event_type()

    is_late = random.random() < LATE_EVENT_PROBABILITY
    if is_late:
        event_time = now - timedelta(seconds=LATE_EVENT_DELAY_SECONDS)
    else:
        event_time = now

    return ClickstreamEvent(
        event_type=event_type,
        user_id=user_id,
        session_id=session_id,
        event_time=event_time,
        ingestion_time=now,
        metadata=_build_metadata(event_type),
    )


def run_producer():
    """
    Main loop: continuously generate events and publish them to Kafka.
    Simulates a pool of concurrent users, each with their own session,
    rather than a single user generating all events.
    """
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    # Simulate a pool of ~20 concurrent "active sessions"
    active_sessions = [
        {"user_id": fake.uuid4(), "session_id": fake.uuid4()}
        for _ in range(20)
    ]

    print(f"🚀 Producing to topic '{KAFKA_TOPIC}' at ~{EVENTS_PER_SECOND} events/sec")
    print(f"   Late event probability: {LATE_EVENT_PROBABILITY*100:.0f}%  "
          f"(delay: {LATE_EVENT_DELAY_SECONDS}s)")

    sent_count = 0
    late_count = 0
    interval = 1.0 / EVENTS_PER_SECOND

    try:
        while True:
            session = random.choice(active_sessions)
            event = generate_event(session["user_id"], session["session_id"])

            producer.produce(
                KAFKA_TOPIC,
                key=event.user_id.encode("utf-8"),
                value=event.to_json_bytes(),
                callback=_delivery_report,
            )
            producer.poll(0)  # trigger delivery callbacks without blocking

            sent_count += 1
            if event.event_time != event.ingestion_time:
                late_count += 1

            # Occasionally rotate a session out for a fresh one —
            # keeps the simulated user pool from going stale forever
            if random.random() < 0.02:
                idx = active_sessions.index(session)
                active_sessions[idx] = {
                    "user_id": fake.uuid4(),
                    "session_id": fake.uuid4(),
                }

            if sent_count % 50 == 0:
                print(f"   sent={sent_count}  late={late_count}")

            time.sleep(interval)

    except KeyboardInterrupt:
        print(f"\n🛑 Stopping. Total sent: {sent_count} (late: {late_count})")
        producer.flush()


if __name__ == "__main__":
    run_producer()
