from betflow.kafka_orch.config.topic_config import TopicConfig, create_default_topics


class TestTopicConfig:
    def test_topic_config_initialization(self):
        """Test topic configuration initialization."""
        topic_config = TopicConfig(
            name="test.topic", partitions=3, replication_factor=1, retention_ms=86400000
        )

        assert topic_config.name == "test.topic"
        assert topic_config.partitions == 3
        assert topic_config.replication_factor == 1
        assert topic_config.retention_ms == 86400000

    def test_topic_config_defaults(self):
        """Test default values in topic configuration."""
        topic_config = TopicConfig(
            name="test.topic", partitions=3, replication_factor=1
        )

        assert topic_config.cleanup_policy == "delete"
        assert topic_config.compression_type == "gzip"
        assert topic_config.retention_ms is None

    def test_topic_configs_property(self):
        """Test topic configs property generation."""
        topic_config = TopicConfig(
            name="test.topic",
            partitions=3,
            replication_factor=1,
            retention_ms=86400000,
            cleanup_policy="compact",
        )

        configs = topic_config.configs
        assert configs["cleanup.policy"] == "compact"
        assert configs["compression.type"] == "gzip"
        assert configs["retention.ms"] == "86400000"

    def test_default_topics_creation(self):
        """Test default topics creation."""
        topics = create_default_topics()

        # Verify game topics
        game_topics = [t for t in topics if "games" in t.name]
        assert len(game_topics) == 2
        assert any(t.name == "sports.games.live" for t in game_topics)
        assert any(t.name == "sports.games.completed" for t in game_topics)

        # Verify odds topics
        odds_topics = [t for t in topics if "odds" in t.name]
        assert len(odds_topics) == 2
        assert any(t.name == "sports.odds.live" for t in odds_topics)
        assert any(t.name == "sports.odds.history" for t in odds_topics)

    def test_topic_retention_policies(self):
        """Test topic retention policies."""
        topics = create_default_topics()

        # Live topics should have 24-hour retention
        live_topics = [t for t in topics if "live" in t.name]
        for topic in live_topics:
            assert topic.retention_ms == 86400000  # 24 hours

        # History topics should have 7-day retention
        history_topics = [t for t in topics if "history" in t.name]
        for topic in history_topics:
            assert topic.retention_ms == 604800000  # 7 days

    def test_topic_partitions_and_replication(self):
        """Test topic partitions and replication factor."""
        topics = create_default_topics()

        for topic in topics:
            assert topic.partitions == 3  # All topics should have 3 partitions
            assert topic.replication_factor == 1  # Development setting
