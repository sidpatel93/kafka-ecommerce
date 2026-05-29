from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from quixstreams import Application
from quixstreams.dataframe.windows import Count

from app.config import get_settings


def _event_time_extractor(
    value: dict[str, Any],
    headers: list[tuple[str, bytes]] | None,
    timestamp: int,
    timestamp_type: int,
) -> int:
    """Prefer event payload time; fall back to Kafka record timestamp."""
    del headers, timestamp_type

    raw_timestamp = value.get("timestamp")
    if not raw_timestamp:
        return int(timestamp)

    try:
        # Producer emits ISO-8601, e.g. 2026-05-28T23:03:00.123456+00:00
        return int(datetime.fromisoformat(str(raw_timestamp)).timestamp() * 1000)
    except ValueError:
        return int(timestamp)


def _is_electronics_purchase(event: dict[str, Any]) -> bool:
    product = event.get("product") or {}
    category = str(product.get("category", "")).lower()
    return event.get("event_type") == "purchase" and category == "electronics"


def _format_electronics_metric(window_result: dict[str, Any]) -> dict[str, Any]:
    return {
        "metric": "electronics_sold_last_hour",
        "window_start_ms": window_result["start"],
        "window_end_ms": window_result["end"],
        "value": window_result["event_count"],
        "group": "electronics",
    }


def _format_user_metric(window_result: dict[str, Any]) -> dict[str, Any]:
    return {
        "metric": "user_events_last_10m",
        "window_start_ms": window_result["start"],
        "window_end_ms": window_result["end"],
        "value": window_result["event_count"],
    }


def run() -> None:
    settings = get_settings()

    app = Application(
        broker_address=settings.kafka_bootstrap_servers,
        consumer_group="analytics-windowed-metrics-v1",
        auto_offset_reset="earliest",
    )

    raw_events_topic = app.topic(
        name=settings.topic_raw_events,
        value_deserializer="json",
        timestamp_extractor=_event_time_extractor,
    )

    analytics_topic = app.topic(
        name=settings.topic_analytics_1m,
        value_serializer="json",
    )

    events = app.dataframe(raw_events_topic)

    # Pipeline 1: get the "electornic" category of product in last hour
    (
        events.filter(_is_electronics_purchase)
        .group_by(key=lambda _event: "electronics", name="category_electronics")
        .hopping_window(duration_ms=timedelta(hours=1), step_ms=timedelta(minutes=1))
        .agg(event_count=Count())
        .current()
        .apply(_format_electronics_metric)
        .to_topic(analytics_topic)
    )

    # Pipeline 2: get the user events in last 10 minutes. 
    (
        events.hopping_window(duration_ms=timedelta(minutes=10), step_ms=timedelta(minutes=1))
        .agg(event_count=Count())
        .current()
        .apply(_format_user_metric)
        .to_topic(analytics_topic)
    )

    print(
        "[analytics] Running windowed metrics processor: "
        "electronics sold (1h) + per-user events (10m)"
    )
    app.run()


def main() -> None:
    run()


if __name__ == "__main__":
    main()
