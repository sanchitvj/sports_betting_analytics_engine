import logging
from typing import List
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from betflow.kafka_orch.config.producer_config import ProducerConfig


class KafkaAdminManager:
    """Manages Kafka administrative operations."""

    def __init__(self, config: ProducerConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.admin_client = self._create_admin_client()

    def _create_admin_client(self) -> KafkaAdminClient:
        """Create Kafka admin client with configuration."""
        try:
            return KafkaAdminClient(
                bootstrap_servers=self.config.bootstrap_servers,
                client_id=f"{self.config.client_id}_admin",
                request_timeout_ms=30000,
                connections_max_idle_ms=300000
            )
        except Exception as e:
            self.logger.error(f"Failed to create admin client: {e}")
            raise

    def ensure_topics_exist(self, topics: List[NewTopic]) -> None:
        """Ensure all required topics exist."""
        try:
            existing_topics = self.admin_client.list_topics()
            topics_to_create = [
                topic for topic in topics
                if topic.name not in existing_topics
            ]

            if topics_to_create:
                try:
                    self.admin_client.create_topics(
                        new_topics=topics_to_create,
                        validate_only=False
                    )
                    self.logger.info(f"Created {len(topics_to_create)} new topics")
                except TopicAlreadyExistsError:
                    self.logger.warning("Some topics already exist")
                except Exception as e:
                    self.logger.error(f"Failed to create topics: {e}")
                    raise

        except Exception as e:
            self.logger.error(f"Error ensuring topics exist: {e}")
            raise

    def delete_topics(self, topic_names: List[str]) -> None:
        """Delete specified topics."""
        try:
            self.admin_client.delete_topics(topic_names)
            self.logger.info(f"Deleted topics: {topic_names}")
        except Exception as e:
            self.logger.error(f"Failed to delete topics: {e}")
            raise

    def close(self) -> None:
        """Close admin client connection."""
        try:
            self.admin_client.close()
        except Exception as e:
            self.logger.error(f"Error closing admin client: {e}")