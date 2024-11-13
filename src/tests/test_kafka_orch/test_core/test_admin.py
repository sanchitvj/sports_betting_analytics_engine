import pytest
from unittest.mock import Mock, patch
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from betflow.kafka_orch.core.admin import KafkaAdminManager


@pytest.fixture
def mock_config():
    """Create mock config with required attributes."""
    config = Mock()
    config.bootstrap_servers = "localhost:9092"
    config.client_id = "test_client"
    return config


@pytest.fixture
def mock_admin_client():
    """Create mock KafkaAdminClient."""
    with patch("kafka.admin.KafkaAdminClient", autospec=True) as mock:
        yield mock


class TestKafkaAdminManager:
    def test_initialization(self, mock_config):
        """Test admin manager initialization. Patching at module level as import location matters for connecting to mock client."""
        with patch("betflow.kafka_orch.core.admin.KafkaAdminClient") as mock_client:
            # Configure the mock
            mock_instance = mock_client.return_value

            # Create admin manager
            admin_manager = KafkaAdminManager(mock_config)

            # Verify KafkaAdminClient was created with correct params
            mock_client.assert_called_once_with(
                bootstrap_servers=mock_config.bootstrap_servers,
                client_id=f"{mock_config.client_id}_admin",
                request_timeout_ms=30000,
                connections_max_idle_ms=300000,
            )
            assert admin_manager.admin_client == mock_instance

    def test_ensure_topics_exist_new_topics(self, mock_config):
        """Test creating new topics."""
        with patch("betflow.kafka_orch.core.admin.KafkaAdminClient") as mock_client:
            # Configure the mock
            mock_instance = mock_client.return_value
            mock_instance.list_topics.return_value = []

            # Create admin manager
            admin_manager = KafkaAdminManager(mock_config)

            # Create test topic config
            topics = [
                NewTopic(name="test_topic", num_partitions=3, replication_factor=1)
            ]

            # Test topic creation
            admin_manager.ensure_topics_exist(topics)

            # Verify create_topics was called
            mock_instance.create_topics.assert_called_once()

    def test_ensure_topics_exist_already_exists(self, mock_config):
        """Test handling existing topics."""
        with patch("betflow.kafka_orch.core.admin.KafkaAdminClient") as mock_client:
            # Configure the mock
            mock_instance = mock_client.return_value
            mock_instance.list_topics.return_value = ["test_topic"]

            # Create admin manager
            admin_manager = KafkaAdminManager(mock_config)

            # Create test topic config
            topics = [
                NewTopic(name="test_topic", num_partitions=3, replication_factor=1)
            ]

            # Test topic creation
            admin_manager.ensure_topics_exist(topics)

            # Verify create_topics was not called
            mock_instance.create_topics.assert_not_called()

    def test_ensure_topics_exist_error_handling(self, mock_config):
        """Test error handling during topic creation."""
        with patch("betflow.kafka_orch.core.admin.KafkaAdminClient") as mock_client:
            # Configure the mock
            mock_instance = mock_client.return_value
            mock_instance.list_topics.return_value = []
            mock_instance.create_topics.side_effect = TopicAlreadyExistsError()

            # Create admin manager
            admin_manager = KafkaAdminManager(mock_config)

            # Create test topic config
            topics = [
                NewTopic(name="test_topic", num_partitions=3, replication_factor=1)
            ]

            # Test topic creation - should not raise exception
            admin_manager.ensure_topics_exist(topics)

    def test_delete_topics(self, mock_config):
        """Test topic deletion."""
        with patch("betflow.kafka_orch.core.admin.KafkaAdminClient") as mock_client:
            # Configure the mock
            mock_instance = mock_client.return_value

            # Create admin manager
            admin_manager = KafkaAdminManager(mock_config)

            # Test topic deletion
            topic_names = ["topic1", "topic2"]
            admin_manager.delete_topics(topic_names)

            # Verify delete_topics was called
            mock_instance.delete_topics.assert_called_once_with(topic_names)

    def test_close(self, mock_config):
        """Test admin client cleanup."""
        with patch("betflow.kafka_orch.core.admin.KafkaAdminClient") as mock_client:
            # Configure the mock
            mock_instance = mock_client.return_value

            # Create admin manager
            admin_manager = KafkaAdminManager(mock_config)

            # Test close
            admin_manager.close()

            # Verify close was called
            mock_instance.close.assert_called_once()
