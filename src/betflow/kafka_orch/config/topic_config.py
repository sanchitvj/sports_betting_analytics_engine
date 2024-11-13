from dataclasses import dataclass
from typing import Optional, List


@dataclass
class TopicConfig:
    name: str
    partitions: int
    replication_factor: int
    retention_ms: Optional[int] = None
    cleanup_policy: str = "delete"
    compression_type: str = "gzip"

    @property
    def configs(self) -> dict:
        configs = {
            "cleanup.policy": self.cleanup_policy,
            "compression.type": self.compression_type,
        }
        if self.retention_ms:
            configs["retention.ms"] = str(self.retention_ms)
        return configs


def create_default_topics() -> List[TopicConfig]:
    """Create default topic configurations."""
    return [
        # Game topics
        TopicConfig(
            name="sports.games.live",
            partitions=3,
            replication_factor=1,
            retention_ms=86400000,  # 24 hours
        ),
        TopicConfig(
            name="sports.games.completed",
            partitions=3,
            replication_factor=1,
            retention_ms=604800000,  # 7 days
        ),
        # Odds topics
        TopicConfig(
            name="sports.odds.live",
            partitions=3,
            replication_factor=1,
            retention_ms=86400000,
        ),
        TopicConfig(
            name="sports.odds.history",
            partitions=3,
            replication_factor=1,
            retention_ms=604800000,
        ),
        # Weather topics
        TopicConfig(
            name="sports.weather.current",
            partitions=3,
            replication_factor=1,
            retention_ms=86400000,
        ),
        # News topics
        TopicConfig(
            name="sports.news.live",
            partitions=3,
            replication_factor=1,
            retention_ms=86400000,
        ),
    ]
