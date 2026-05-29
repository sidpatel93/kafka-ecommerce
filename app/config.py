from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "kafka:9092"

    topic_raw_events: str = "ecommerce.events.raw"
    topic_validated_events: str = "ecommerce.events.validated"
    topic_dlq: str = "ecommerce.events.dlq"
    topic_analytics_1m: str = "ecommerce.analytics.1m"
    topic_alerts: str = "ecommerce.alerts"

    topic_num_partitions: int = 1
    topic_replication_factor: int = 1
    topic_retention_ms: int = 300000

    topic_init_max_retries: int = 3
    topic_init_retry_delay_seconds: float = 2.0

    producer_events_per_second: int = 1

    @property
    def kafka_topics(self) -> list[str]:
        return [
            self.topic_raw_events,
            self.topic_validated_events,
            self.topic_dlq,
            self.topic_analytics_1m,
            self.topic_alerts,
        ]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
