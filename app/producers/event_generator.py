from __future__ import annotations

import json
import random
import signal
import time
import uuid
from datetime import UTC, datetime

from confluent_kafka import Producer
from faker import Faker

from app.config import get_settings

EVENT_TYPES: tuple[str, ...] = (
    "page_view",
    "add_to_cart",
    "remove_from_cart",
    "checkout_started",
    "purchase",
    "refund",
)

EVENT_WEIGHTS: tuple[int, ...] = (40, 20, 8, 10, 18, 4)

PRODUCT_CATEGORIES: tuple[str, ...] = (
    "electronics",
    "fashion",
    "home",
    "sports",
    "beauty",
    "books",
)

TRAFFIC_SOURCES: tuple[str, ...] = (
    "direct",
    "search",
    "email",
    "social",
    "ad_campaign",
)

PAYMENT_METHODS: tuple[str, ...] = ("card", "paypal", "apple_pay", "google_pay")


def _utc_timestamp() -> str:
    return datetime.now(UTC).isoformat()


def _build_event(fake: Faker) -> dict[str, object]:
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
    quantity = random.randint(1, 4)
    unit_price = round(random.uniform(8.0, 500.0), 2)

    product_id = f"prd_{random.randint(1000, 9999)}"
    user_id = f"usr_{random.randint(10000, 99999)}"

    event: dict[str, object] = {
        "event_id": str(uuid.uuid4()),
        "timestamp": _utc_timestamp(),
        "event_type": event_type,
        "user_id": user_id,
        "session_id": str(uuid.uuid4()),
        "product": {
            "product_id": product_id,
            "name": fake.word().title(),
            "category": random.choice(PRODUCT_CATEGORIES),
            "unit_price": unit_price,
            "quantity": quantity,
        },
        "source": {
            "traffic_source": random.choice(TRAFFIC_SOURCES),
            "device": random.choice(("web", "ios", "android")),
            "country": fake.country_code(),
        },
    }

    if event_type in {"purchase", "refund"}:
        event["order"] = {
            "order_id": f"ord_{random.randint(100000, 999999)}",
            "payment_method": random.choice(PAYMENT_METHODS),
            "total_amount": round(unit_price * quantity, 2),
        }

    return event


def _delivery_report(err, msg) -> None:
    if err is not None:
        print(f"[producer] Delivery failed: {err}")


def _producer_config(bootstrap_servers: str) -> dict[str, object]:
    return {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "mock-ecommerce-event-generator",
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
        "linger.ms": 50,
    }


def run() -> None:
    settings = get_settings()
    fake = Faker()

    events_per_second = max(1, settings.producer_events_per_second)
    if events_per_second != settings.producer_events_per_second:
        print("[producer] PRODUCER_EVENTS_PER_SECOND must be > 0. Falling back to 1.")

    interval_seconds = 1.0 / events_per_second

    producer = Producer(_producer_config(settings.kafka_bootstrap_servers))

    running = True

    def _stop_handler(signum, _frame) -> None:
        nonlocal running
        running = False
        print(f"[producer] Received signal {signum}. Stopping producer loop...")

    signal.signal(signal.SIGINT, _stop_handler)
    signal.signal(signal.SIGTERM, _stop_handler)

    print(
        "[producer] Producing events to "
        f"'{settings.topic_raw_events}' at {events_per_second} event(s)/sec"
    )

    produced_count = 0

    while running:
        event = _build_event(fake)
        message_key = str(event["user_id"])
        message_value = json.dumps(event)

        try:
            producer.produce(
                topic=settings.topic_raw_events,
                key=message_key,
                value=message_value,
                on_delivery=_delivery_report,
            )
            produced_count += 1
        except BufferError:
            # Local queue is full; allow the producer to serve delivery callbacks.
            producer.poll(0.25)
            continue

        # Serve delivery callbacks and broker communication.
        producer.poll(0)

        if produced_count % max(1, events_per_second * 5) == 0:
            print(f"[producer] Produced {produced_count} event(s)")

        time.sleep(interval_seconds)

    print(f"[producer] Flushing messages ({produced_count} produced)...")
    producer.flush(timeout=10)
    print("[producer] Shutdown complete")


def main() -> None:
    run()


if __name__ == "__main__":
    main()
