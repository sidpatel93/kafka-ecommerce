from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "kafka:9092"

    topic_raw_events: str
    topic_validated_events: str
    topic_dlq: str
    topic_analytics_1m: str
    topic_alerts: str

    topic_num_partitions: int = 1
    topic_replication_factor: int = 1

    topic_init_max_retries: int = 30
    topic_init_retry_delay_seconds: float = 2.0

    producer_events_per_second: int = 10

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

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
