import os
import json
import time
import feedparser
from typing import Dict, Any, Optional, List
import requests
from kafka import KafkaProducer
from betflow.api_connectors.conn_utils import RateLimiter


class NewsAPIConnector:
    """Connector for NewsAPI with Kafka integration."""

    def __init__(
        self,
        api_key: str,
        kafka_bootstrap_servers: str,
        base_url: str = "https://newsapi.org/v2",
    ) -> None:
        """Initialize NewsAPI connector.

        Args:
            api_key: NewsAPI key
            kafka_bootstrap_servers: Kafka bootstrap servers
            base_url: Base URL for NewsAPI
        """
        self.api_key = api_key
        self.base_url = base_url
        self.session = requests.Session()
        # Free tier: 100 requests per day
        self.rate_limiter = RateLimiter(requests_per_second=0.001)

        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            compression_type="gzip",
            retries=3,
            acks="all",
        )

    def make_request(
        self, endpoint: str, params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Make a rate-limited request to NewsAPI."""
        self.rate_limiter.wait_if_needed()

        if params is None:
            params = {}
        params["apiKey"] = self.api_key

        url = f"{self.base_url}/{endpoint}"

        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                raise Exception("Rate limit exceeded")
            raise Exception(f"HTTP error occurred: {e}")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Request failed: {e}")

    @staticmethod
    def api_raw_news_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw news data to our schema."""
        return {
            "status": raw_data.get("status"),
            "total_results": raw_data.get("totalResults"),
            "articles": [
                {
                    "source": article.get("source", {}).get("name"),
                    "author": article.get("author"),
                    "title": article.get("title"),
                    "description": article.get("description"),
                    "url": article.get("url"),
                    "published_at": article.get("publishedAt"),
                    "content": article.get("content"),
                }
                for article in raw_data.get("articles", [])
            ],
            "timestamp": int(time.time()),
        }

    def publish_to_kafka(self, topic: str, data: Dict[str, Any]) -> None:
        """Publish data to Kafka topic."""
        try:
            future = self.producer.send(topic, value=data)
            future.get(timeout=10)
        except Exception as e:
            raise Exception(f"Failed to publish to Kafka: {e}")

    def fetch_and_publish_news(
        self,
        query: str,
        topic_name: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        language: str = "en",
        sort_by: str = "publishedAt",
    ) -> None:
        """Fetch news data and publish to Kafka."""
        try:
            params = {"q": query, "language": language, "sortBy": sort_by}
            if from_date:
                params["from"] = from_date
            if to_date:
                params["to"] = to_date

            raw_data = self.make_request("everything", params=params)
            transformed_data = self.api_raw_news_data(raw_data)
            self.publish_to_kafka(topic_name, transformed_data)

        except Exception as e:
            raise Exception(f"Failed to fetch and publish news: {e}")

    def close(self) -> None:
        """Clean up resources."""
        self.producer.flush()
        self.producer.close()
        self.session.close()


class GNewsConnector:
    """Connector for GNews API with Kafka integration."""

    def __init__(
        self,
        api_key: str,
        kafka_bootstrap_servers: str,
        base_url: str = "https://gnews.io/api/v4",
    ) -> None:
        """Initialize GNews connector."""
        self.api_key = api_key
        self.base_url = base_url
        self.session = requests.Session()
        # Free tier: 100 requests per day
        self.rate_limiter = RateLimiter(requests_per_second=0.001)

        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            compression_type="gzip",
            retries=3,
            acks="all",
        )

    def make_request(
        self, endpoint: str, params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Make a rate-limited request to GNews API."""
        self.rate_limiter.wait_if_needed()

        if params is None:
            params = {}
        params["token"] = self.api_key

        url = f"{self.base_url}/{endpoint}"

        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                raise Exception("Rate limit exceeded")
            raise Exception(f"HTTP error occurred: {e}")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Request failed: {e}")

    @staticmethod
    def api_raw_news_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw news data to our schema."""
        return {
            "total_articles": raw_data.get("totalArticles"),
            "articles": [
                {
                    "title": article.get("title"),
                    "description": article.get("description"),
                    "content": article.get("content"),
                    "url": article.get("url"),
                    "image": article.get("image"),
                    "published_at": article.get("publishedAt"),
                    "source": {
                        "name": article.get("source", {}).get("name"),
                        "url": article.get("source", {}).get("url"),
                    },
                }
                for article in raw_data.get("articles", [])
            ],
            "timestamp": int(time.time()),
        }

    def publish_to_kafka(self, topic: str, data: Dict[str, Any]) -> None:
        """Publish data to Kafka topic."""
        try:
            future = self.producer.send(topic, value=data)
            future.get(timeout=10)
        except Exception as e:
            raise Exception(f"Failed to publish to Kafka: {e}")

    def fetch_and_publish_news(
        self, query: str, lang: str = "en", country: str = "us", max_articles: int = 10
    ) -> None:
        """Fetch news data and publish to Kafka."""
        try:
            params = {"q": query, "lang": lang, "country": country, "max": max_articles}

            raw_data = self.make_request("search", params=params)
            transformed_data = self.api_raw_news_data(raw_data)
            self.publish_to_kafka(f"news.gnews.{query}", transformed_data)

        except Exception as e:
            raise Exception(f"Failed to fetch and publish news: {e}")

    def close(self) -> None:
        """Clean up resources."""
        self.producer.flush()
        self.producer.close()
        self.session.close()


class RSSFeedConnector:
    """Connector for RSS feeds with Kafka integration."""

    def __init__(self, kafka_bootstrap_servers: str, feed_urls: List[str]) -> None:
        """Initialize RSS feed connector."""
        self.feed_urls = feed_urls
        self.rate_limiter = RateLimiter(requests_per_second=0.1)

        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            compression_type="gzip",
            retries=3,
            acks="all",
        )

    def fetch_feed(self, url: str) -> Dict[str, Any]:
        """Fetch and parse RSS feed."""
        self.rate_limiter.wait_if_needed()

        try:
            feed = feedparser.parse(url)
            if feed.get("bozo_exception"):
                raise Exception(f"Feed parsing error: {feed['bozo_exception']}")
            return feed

        except Exception as e:
            raise Exception(f"Failed to fetch feed {url}: {e}")

    @staticmethod
    def api_raw_feed_data(feed: Dict[str, Any]) -> Dict[str, Any]:
        """Transform RSS feed data to our schema."""
        return {
            "feed_info": {
                "title": feed["feed"].get("title"),
                "link": feed["feed"].get("link"),
                "description": feed["feed"].get("description"),
                "language": feed["feed"].get("language"),
            },
            "entries": [
                {
                    "title": entry.get("title"),
                    "link": entry.get("link"),
                    "description": entry.get("description"),
                    "published": entry.get("published"),
                    "author": entry.get("author"),
                    "tags": [tag.get("term") for tag in entry.get("tags", [])],
                }
                for entry in feed.get("entries", [])
            ],
            "timestamp": int(time.time()),
        }

    def publish_to_kafka(self, topic: str, data: Dict[str, Any]) -> None:
        """Publish data to Kafka topic."""
        try:
            future = self.producer.send(topic, value=data)
            future.get(timeout=10)
        except Exception as e:
            raise Exception(f"Failed to publish to Kafka: {e}")

    def fetch_and_publish_feeds(self) -> None:
        """Fetch all RSS feeds and publish to Kafka."""
        for url in self.feed_urls:
            try:
                feed = self.fetch_feed(url)
                transformed_data = self.api_raw_feed_data(feed)

                # Create topic name from feed URL
                topic = f"news.rss.{url.replace('/', '_').replace(':', '')}"
                self.publish_to_kafka(topic, transformed_data)

            except Exception as e:
                print(f"Error processing feed {url}: {e}")

    def close(self) -> None:
        """Clean up resources."""
        self.producer.flush()
        self.producer.close()


def main():
    """Main function to demonstrate usage."""
    newsapi_key = os.getenv("NEWSAPI_KEY")
    gnews_key = os.getenv("GNEWS_API_KEY")

    # Initialize connectors
    newsapi = NewsAPIConnector(
        api_key=newsapi_key, kafka_bootstrap_servers="localhost:9092"
    )

    gnews = GNewsConnector(api_key=gnews_key, kafka_bootstrap_servers="localhost:9092")

    rss = RSSFeedConnector(
        kafka_bootstrap_servers="localhost:9092",
        feed_urls=[
            "http://feeds.bbci.co.uk/news/rss.xml",
            "http://rss.cnn.com/rss/cnn_topstories.rss",
        ],
    )

    try:
        # Fetch news from different sources
        newsapi.fetch_and_publish_news("sports betting")
        gnews.fetch_and_publish_news("sports analytics")
        rss.fetch_and_publish_feeds()

    except Exception as e:
        print(f"Error: {e}")
    finally:
        newsapi.close()
        gnews.close()
        rss.close()


if __name__ == "__main__":
    main()
