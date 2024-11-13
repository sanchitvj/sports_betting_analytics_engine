import pytest
from datetime import datetime
from betflow.spark_streaming.event_transformer import NewsTransformer


class TestNewsTransformer:
    @pytest.fixture
    def news_transformer(self):
        """Create NewsTransformer instance."""
        return NewsTransformer()

    @pytest.fixture
    def newsapi_data(self):
        """Fixture for NewsAPI data."""
        return {
            "source": {"id": "espn", "name": "ESPN"},
            "author": "John Doe",
            "title": "NBA Finals Preview",
            "description": "A preview of the upcoming finals",
            "content": "Detailed analysis of the matchup...",
            "url": "https://espn.com/nba-finals-preview",
            "publishedAt": "2024-03-14T15:30:00Z",
            "category": "sports",
            "language": "en",
            "sentiment": {"score": 0.8},
            "entities": {
                "teams": ["Lakers", "Celtics"],
                "players": ["LeBron James", "Jayson Tatum"],
            },
        }

    @pytest.fixture
    def gnews_data(self):
        """Fixture for GNews data."""
        return {
            "title": "NFL Draft Analysis",
            "description": "Breaking down top prospects...",
            "url": "https://sports.com/nfl-draft",
            "publishedAt": "2024-03-14T14:00:00Z",
            "source": {"name": "Sports News", "id": "sports-news"},
            "image": "https://example.com/image.jpg",
            "language": "en",
            "sentiment": {"score": 0.6},
        }

    @pytest.fixture
    def rss_data(self):
        """Fixture for RSS feed data."""
        return {
            "title": "MLB Season Preview",
            "summary": "What to expect this season...",
            "link": "https://mlb.com/preview",
            "published": "Thu, 14 Mar 2024 16:00:00 +0000",
            "author": "Jane Smith",
            "feed_name": "MLB News",
            "feed_id": "mlb_news",
            "guid": "12345",
            "tags": ["baseball", "mlb"],
            "categories": ["sports", "baseball"],
        }

    def test_transform_newsapi_valid_data(self, news_transformer, newsapi_data):
        """Test NewsAPI data transformation with valid data."""
        result = news_transformer.transform_newsapi(newsapi_data)

        # Verify basic fields
        assert isinstance(result["news_id"], str)
        assert result["source"] == newsapi_data["source"]["name"]
        assert isinstance(result["published_date"], datetime)
        assert result["title"] == newsapi_data["title"]
        assert result["content"] == newsapi_data["content"]
        assert result["url"] == newsapi_data["url"]
        assert result["author"] == newsapi_data["author"]

        # Verify extracted fields
        assert "sports" in result["categories"]
        assert len(result["entities"]["teams"]) == 2
        assert result["sentiment_score"] == newsapi_data["sentiment"]["score"]

        # Verify metadata
        assert result["metadata"]["source_id"] == newsapi_data["source"]["id"]
        assert result["metadata"]["language"] == newsapi_data["language"]

    def test_transform_newsapi_missing_optional(self, news_transformer, newsapi_data):
        """Test NewsAPI transformation with missing optional fields."""
        # Remove optional fields
        del newsapi_data["author"]
        del newsapi_data["sentiment"]
        del newsapi_data["entities"]

        result = news_transformer.transform_newsapi(newsapi_data)

        assert result["author"] is None
        assert result["sentiment_score"] is None
        assert result["entities"]["teams"] == []
        assert result["entities"]["players"] == []

    def test_transform_gnews_valid_data(self, news_transformer, gnews_data):
        """Test GNews data transformation with valid data."""
        result = news_transformer.transform_gnews(gnews_data)

        # Verify basic fields
        assert isinstance(result["news_id"], str)
        assert result["source"] == gnews_data["source"]["name"]
        assert isinstance(result["published_date"], datetime)
        assert result["title"] == gnews_data["title"]
        assert result["content"] == gnews_data["description"]
        assert result["url"] == gnews_data["url"]

        # Verify metadata
        assert result["metadata"]["source_id"] == gnews_data["source"]["id"]
        assert result["metadata"]["language"] == gnews_data["language"]
        assert result["metadata"]["image_url"] == gnews_data["image"]

    def test_transform_rss_valid_data(self, news_transformer, rss_data):
        """Test RSS feed data transformation with valid data."""
        result = news_transformer.transform_rss(rss_data)

        # Verify basic fields
        assert isinstance(result["news_id"], str)
        assert result["source"] == rss_data["feed_name"]
        assert isinstance(result["published_date"], datetime)
        assert result["title"] == rss_data["title"]
        assert result["content"] == rss_data["summary"]
        assert result["url"] == rss_data["link"]
        assert result["author"] == rss_data["author"]

        # Verify categories and metadata
        assert "baseball" in result["categories"]
        assert "mlb" in result["categories"]
        assert result["metadata"]["feed_id"] == rss_data["feed_id"]
        assert result["metadata"]["guid"] == rss_data["guid"]

    def test_generate_news_id(self, news_transformer):
        """Test news ID generation."""
        url1 = "https://example.com/article1"
        url2 = "https://example.com/article2"

        # Test uniqueness
        id1 = news_transformer._generate_news_id(url1)
        id2 = news_transformer._generate_news_id(url2)
        assert id1 != id2

        # Test consistency
        assert news_transformer._generate_news_id(url1) == id1

    def test_extract_categories(self, news_transformer):
        """Test category extraction."""
        data = {"category": "sports", "tags": ["nba", "basketball"]}

        categories = news_transformer._extract_categories(data)
        assert "sports" in categories
        assert "nba" in categories
        assert "basketball" in categories
        assert len(categories) == 3  # No duplicates

    def test_extract_entities(self, news_transformer):
        """Test entity extraction."""
        data = {
            "entities": {
                "teams": ["Lakers", "Celtics"],
                "players": ["LeBron James"],
                "locations": ["Los Angeles"],
            }
        }

        entities = news_transformer._extract_entities(data)
        assert len(entities["teams"]) == 2
        assert len(entities["players"]) == 1
        assert len(entities["locations"]) == 1
        assert "Lakers" in entities["teams"]
        assert "LeBron James" in entities["players"]

    def test_calculate_relevance_score(self, news_transformer):
        """Test relevance score calculation."""
        # Test with direct relevance score
        assert news_transformer._calculate_relevance_score({"relevance": 0.8}) == 0.8

        # Test with alternative score
        assert news_transformer._calculate_relevance_score({"score": 0.7}) == 0.7

        # Test with missing score
        assert news_transformer._calculate_relevance_score({}) is None

    def test_parse_rss_date(self, news_transformer):
        """Test RSS date parsing."""
        # Test RFC 822 format
        rfc822_date = "Wed, 14 Mar 2024 15:30:00 +0000"
        parsed = news_transformer._parse_rss_date(rfc822_date)
        assert isinstance(parsed, datetime)

        # Test ISO format
        iso_date = "2024-03-14T15:30:00+00:00"
        parsed = news_transformer._parse_rss_date(iso_date)
        assert isinstance(parsed, datetime)

        # Test invalid format (should return current time)
        invalid_date = "invalid date format"
        parsed = news_transformer._parse_rss_date(invalid_date)
        assert isinstance(parsed, datetime)
        assert (datetime.now() - parsed).total_seconds() < 1

    def test_error_handling(self, news_transformer):
        """Test error handling in transformations."""
        # Test NewsAPI with invalid data
        with pytest.raises(ValueError):
            news_transformer.transform_newsapi({})

        # Test GNews with invalid data
        with pytest.raises(ValueError):
            news_transformer.transform_gnews({})

        # Test RSS with invalid data
        with pytest.raises(ValueError):
            news_transformer.transform_rss({})
