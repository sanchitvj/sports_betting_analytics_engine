import pytest
from unittest.mock import Mock, patch
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from betflow.kafka_orch.core.admin import KafkaAdminManager
from betflow.kafka_orch.config.producer_config import ProducerConfig


class TestKafkaAdminManager:
    @pytest.fixture
    def mock_config(self):
        config = Mock(spec=ProducerConfig)
        config.bootstrap_servers = "localhost:9092"
        config.client_id = "test_client"
        return config

    @pytest.fixture
    def mock_admin_client(self):
        with patch('kafka.admin.KafkaAdminClient') as mock:
            yield mock

    def test_initialization(self, mock_config, mock_admin_client):
        """Test admin manager initialization."""
        admin_manager = KafkaAdminManager(mock_config)

        mock_admin_client.assert_called_once_with(
            bootstrap_servers=mock_config.bootstrap_servers,
            client_id=f"{mock_config.client_id}_admin",
            request_timeout_ms=30000,
            connections_max_idle_ms=300000
        )

    def test_ensure_topics_exist_new_topics(self, mock_config):
        """Test creating new topics."""
        admin_manager = KafkaAdminManager(mock_config)
        admin_manager.admin_client.list_topics = Mock(return_value=[])
        admin_manager.admin_client.create_topics = Mock()

        topics = [
            NewTopic(
                name="test_topic",
                num_partitions=3,
                replication_factor=1
            )
        ]

        admin_manager.ensure_topics_exist(topics)

        admin_manager.admin_client.create_topics.assert_called_once()

    def test_ensure_topics_exist_already_exists(self, mock_config):
        """Test handling existing topics."""
        admin_manager = KafkaAdminManager(mock_config)
        admin_manager.admin_client.list_topics = Mock(return_value=["test_topic"])
        admin_manager.admin_client.create_topics = Mock()

        topics = [
            NewTopic(
                name="test_topic",
                num_partitions=3,
                replication_factor=1
            )
        ]

        admin_manager.ensure_topics_exist(topics)

        admin_manager.admin_client.create_topics.assert_not_called()

    def test_ensure_topics_exist_error_handling(self, mock_config):
        """Test error handling during topic creation."""
        admin_manager = KafkaAdminManager(mock_config)
        admin_manager.admin_client.list_topics = Mock(return_value=[])
        admin_manager.admin_client.create_topics = Mock(
            side_effect=TopicAlreadyExistsError()
        )

        topics = [
            NewTopic(
                name="test_topic",
                num_partitions=3,
                replication_factor=1
            )
        ]

        # Should not raise exception
        admin_manager.ensure_topics_exist(topics)

    def test_delete_topics(self, mock_config):
        """Test topic deletion."""
        admin_manager = KafkaAdminManager(mock_config)
        admin_manager.admin_client.delete_topics = Mock()

        topic_names = ["topic1", "topic2"]
        admin_manager.delete_topics(topic_names)

        admin_manager.admin_client.delete_topics.assert_called_once_with(topic_names)

    def test_close(self, mock_config):
        """Test admin client cleanup."""
        admin_manager = KafkaAdminManager(mock_config)
        admin_manager.admin_client.close = Mock()

        admin_manager.close()

        admin_manager.admin_client.close.assert_called_once()