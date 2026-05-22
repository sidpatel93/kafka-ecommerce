import sys
import time

from confluent_kafka.admin import AdminClient, NewTopic

from app.config import get_settings


def _topic_exists(admin_client: AdminClient, topic_name: str) -> bool:
    metadata = admin_client.list_topics(topic=topic_name, timeout=10)
    return topic_name in metadata.topics and metadata.topics[topic_name].error is None


def create_missing_topics() -> int:
    settings = get_settings()
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})

    print("[topic-init] Starting topic initialization for: " + ", ".join(settings.kafka_topics))

    existing_topics: set[str] = set()
    for attempt in range(1, settings.topic_init_max_retries + 1):
        try:
            metadata = admin.list_topics(timeout=10)
            existing_topics = set(metadata.topics.keys())
            break
        except Exception as exc:  # pragma: no cover - relies on container runtime
            retry_window = f"{attempt}/{settings.topic_init_max_retries}"
            print(f"[topic-init] Kafka not ready (attempt {retry_window}): {exc}")
            if attempt == settings.topic_init_max_retries:
                print("[topic-init] Failed to connect to Kafka before retry limit.")
                return 1
            time.sleep(settings.topic_init_retry_delay_seconds)

    topics_to_create = [
        NewTopic(
            topic=name,
            num_partitions=settings.topic_num_partitions,
            replication_factor=settings.topic_replication_factor,
            config={"retention.ms": str(settings.topic_retention_ms)},
        )
        for name in settings.kafka_topics
        if name not in existing_topics
    ]

    if not topics_to_create:
        print("[topic-init] All configured topics already exist.")
        return 0

    futures = admin.create_topics(topics_to_create)
    failed = False

    for topic_name, future in futures.items():
        try:
            future.result()
            print(f"[topic-init] Created topic: {topic_name}")
        except Exception as exc:
            if _topic_exists(admin, topic_name):
                print(f"[topic-init] Topic already exists: {topic_name}")
            else:
                failed = True
                print(f"[topic-init] Failed to create topic '{topic_name}': {exc}")

    if failed:
        return 1

    print("[topic-init] Topic initialization complete.")
    return 0


def main() -> None:
    sys.exit(create_missing_topics())


if __name__ == "__main__":
    main()
