# test_news_connectors.py
import pytest
from unittest.mock import patch, MagicMock
import requests
from betflow.api_connectors.news_conn import NewsAPIConnector, GNewsConnector, RSSFeedConnector


class MockResponse:
    """Mock HTTP response"""

    def __init__(self, json_data, status_code=200, headers=None):
        self.json_data = json_data
        self.status_code = status_code
        self.headers = headers or {}
        self.ok = status_code < 400

    def json(self):
        return self.json_data

    def raise_for_status(self):
        if not self.ok:
            raise requests.exceptions.HTTPError(
                f"{self.status_code} Error",
                response=self
            )


@pytest.fixture(autouse=True)
def mock_kafka():
    """Mock KafkaProducer at module level"""
    with patch('betflow.api_connectors.news_conn.KafkaProducer') as mock:
        mock_instance = MagicMock()
        mock_instance.send.return_value.get.return_value = None
        mock.return_value = mock_instance
        yield mock_instance


class TestNewsAPIConnector:
    @pytest.fixture
    def newsapi_connector(self):
        """Create NewsAPIConnector instance"""
        return NewsAPIConnector(
            api_key="test_api_key",
            kafka_bootstrap_servers='localhost:9092'
        )

    def test_init(self, newsapi_connector):
        """Test connector initialization"""
        assert newsapi_connector.api_key == "test_api_key"
        assert newsapi_connector.base_url == "https://newsapi.org/v2"
        assert newsapi_connector.producer is not None

    def test_make_request_success(self, newsapi_connector):
        """Test successful API request"""
        mock_response = {
            "status": "ok",
            "totalResults": 2,
            "articles": [
                {
                    "source": {"name": "Test Source"},
                    "author": "Test Author",
                    "title": "Test Title",
                    "description": "Test Description",
                    "url": "https://test.com",
                    "publishedAt": "2024-02-09T12:00:00Z",
                    "content": "Test content"
                }
            ]
        }

        session_mock = MagicMock(spec=requests.Session)
        response_mock = MockResponse(mock_response)
        session_mock.get.return_value = response_mock

        with patch('requests.Session', return_value=session_mock):
            newsapi_connector.session = session_mock
            response = newsapi_connector.make_request("everything", {"q": "sports"})

            assert response == mock_response
            session_mock.get.assert_called_once()

    def test_transform_news_data(self, newsapi_connector):
        """Test news data transformation"""
        raw_data = {
            "status": "ok",
            "totalResults": 2,
            "articles": [
                {
                    "source": {"name": "Test Source"},
                    "author": "Test Author",
                    "title": "Test Title",
                    "description": "Test Description",
                    "url": "https://test.com",
                    "publishedAt": "2024-02-09T12:00:00Z",
                    "content": "Test content"
                }
            ]
        }

        transformed = newsapi_connector.transform_news_data(raw_data)

        assert transformed["status"] == "ok"
        assert transformed["total_results"] == 2
        assert len(transformed["articles"]) == 1
        assert transformed["articles"][0]["title"] == "Test Title"
        assert "timestamp" in transformed

    def test_publish_to_kafka_success(self, newsapi_connector):
        """Test successful Kafka message publishing"""
        test_data = {"test": "data"}
        newsapi_connector.publish_to_kafka("test-topic", test_data)

        newsapi_connector.producer.send.assert_called_once()
        args, kwargs = newsapi_connector.producer.send.call_args
        assert args[0] == "test-topic"
        assert kwargs["value"] == test_data

    def test_close(self, newsapi_connector):
        """Test connector cleanup"""
        newsapi_connector.close()
        newsapi_connector.producer.flush.assert_called_once()
        newsapi_connector.producer.close.assert_called_once()


class TestGNewsConnector:
    @pytest.fixture
    def gnews_connector(self):
        """Create GNewsConnector instance"""
        return GNewsConnector(
            api_key="test_api_key",
            kafka_bootstrap_servers='localhost:9092'
        )

    def test_init(self, gnews_connector):
        """Test connector initialization"""
        assert gnews_connector.api_key == "test_api_key"
        assert gnews_connector.base_url == "https://gnews.io/api/v4"
        assert gnews_connector.producer is not None

    def test_make_request_success(self, gnews_connector):
        """Test successful API request"""
        mock_response = {
            "totalArticles": 2,
            "articles": [
                {
                    "title": "Test Title",
                    "description": "Test Description",
                    "content": "Test Content",
                    "url": "https://test.com",
                    "image": "https://test.com/image.jpg",
                    "publishedAt": "2024-02-09T12:00:00Z",
                    "source": {
                        "name": "Test Source",
                        "url": "https://test.com"
                    }
                }
            ]
        }

        session_mock = MagicMock(spec=requests.Session)
        response_mock = MockResponse(mock_response)
        session_mock.get.return_value = response_mock

        with patch('requests.Session', return_value=session_mock):
            gnews_connector.session = session_mock
            response = gnews_connector.make_request("search", {"q": "sports"})

            assert response == mock_response
            session_mock.get.assert_called_once()

    def test_transform_news_data(self, gnews_connector):
        """Test news data transformation"""
        raw_data = {
            "totalArticles": 2,
            "articles": [
                {
                    "title": "Test Title",
                    "description": "Test Description",
                    "content": "Test Content",
                    "url": "https://test.com",
                    "image": "https://test.com/image.jpg",
                    "publishedAt": "2024-02-09T12:00:00Z",
                    "source": {
                        "name": "Test Source",
                        "url": "https://test.com"
                    }
                }
            ]
        }

        transformed = gnews_connector.transform_news_data(raw_data)

        assert transformed["total_articles"] == 2
        assert len(transformed["articles"]) == 1
        assert transformed["articles"][0]["title"] == "Test Title"
        assert "timestamp" in transformed

    def test_publish_to_kafka_success(self, gnews_connector):
        """Test successful Kafka message publishing"""
        test_data = {"test": "data"}
        gnews_connector.publish_to_kafka("test-topic", test_data)

        gnews_connector.producer.send.assert_called_once()
        args, kwargs = gnews_connector.producer.send.call_args
        assert args[0] == "test-topic"
        assert kwargs["value"] == test_data

    def test_close(self, gnews_connector):
        """Test connector cleanup"""
        gnews_connector.close()
        gnews_connector.producer.flush.assert_called_once()
        gnews_connector.producer.close.assert_called_once()


class TestRSSFeedConnector:
    @pytest.fixture
    def rss_connector(self):
        """Create RSSFeedConnector instance"""
        return RSSFeedConnector(
            kafka_bootstrap_servers='localhost:9092',
            feed_urls=["http://test.com/feed.xml"]
        )

    def test_init(self, rss_connector):
        """Test connector initialization"""
        assert rss_connector.feed_urls == ["http://test.com/feed.xml"]
        assert rss_connector.producer is not None

    @patch('feedparser.parse')
    def test_fetch_feed_success(self, mock_parse, rss_connector):
        """Test successful feed fetch"""
        mock_feed = {
            'feed': {
                'title': 'Test Feed',
                'link': 'http://test.com',
                'description': 'Test Description',
                'language': 'en'
            },
            'entries': [
                {
                    'title': 'Test Entry',
                    'link': 'http://test.com/entry',
                    'description': 'Test Entry Description',
                    'published': '2024-02-09T12:00:00Z',
                    'author': 'Test Author',
                    'tags': [{'term': 'sports'}]
                }
            ]
        }
        mock_parse.return_value = mock_feed

        feed = rss_connector.fetch_feed("http://test.com/feed.xml")
        assert feed == mock_feed
        mock_parse.assert_called_once_with("http://test.com/feed.xml")

    def test_transform_feed_data(self, rss_connector):
        """Test feed data transformation"""
        raw_feed = {
            'feed': {
                'title': 'Test Feed',
                'link': 'http://test.com',
                'description': 'Test Description',
                'language': 'en'
            },
            'entries': [
                {
                    'title': 'Test Entry',
                    'link': 'http://test.com/entry',
                    'description': 'Test Entry Description',
                    'published': '2024-02-09T12:00:00Z',
                    'author': 'Test Author',
                    'tags': [{'term': 'sports'}]
                }
            ]
        }

        transformed = rss_connector.transform_feed_data(raw_feed)

        # Add assertions to verify the transformation
        assert transformed['feed_info']['title'] == 'Test Feed'
        assert transformed['feed_info']['link'] == 'http://test.com'
        assert transformed['feed_info']['description'] == 'Test Description'
        assert transformed['feed_info']['language'] == 'en'
        assert len(transformed['entries']) == 1
        assert transformed['entries'][0]['title'] == 'Test Entry'
        assert transformed['entries'][0]['tags'] == ['sports']
        assert 'timestamp' in transformed

    def test_publish_to_kafka_success(self, rss_connector):
        """Test successful Kafka message publishing"""
        test_data = {"test": "data"}
        rss_connector.publish_to_kafka("test-topic", test_data)

        rss_connector.producer.send.assert_called_once()
        args, kwargs = rss_connector.producer.send.call_args
        assert args[0] == "test-topic"
        assert kwargs["value"] == test_data

    def test_close(self, rss_connector):
        """Test connector cleanup"""
        rss_connector.close()
        rss_connector.producer.flush.assert_called_once()
        rss_connector.producer.close.assert_called_once()