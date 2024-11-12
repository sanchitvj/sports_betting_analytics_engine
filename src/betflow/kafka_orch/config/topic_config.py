from dataclasses import dataclass
from typing import Optional, Dict, List

@dataclass
class TopicConfig:
    name: str
    partitions: int
    replication_factor: int
    retention_ms: Optional[int] = None
    cleanup_policy: str = "delete"
    compression_type: str = "gzip"

    @property
    def configs(self) -> Dict[str, str]:
        configs = {
            'cleanup.policy': self.cleanup_policy,
            'compression.type': self.compression_type
        }
        if self.retention_ms:
            configs['retention.ms'] = str(self.retention_ms)
        return configs


def create_default_topics() -> List[TopicConfig]:
    pass
    # Topic creation implementation...