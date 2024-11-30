from typing import Dict, Any, Optional, List
from datetime import datetime
import hashlib
from betflow.kafka_orch.schemas import NewsData


class NewsTransformer:
    """Transform news data from different sources."""

    def transform_newsapi(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform NewsAPI data to standardized format."""
        try:
            # Generate unique news ID
            news_id = self._generate_news_id(raw_data["url"])

            news_data = {
                "news_id": news_id,
                "source": raw_data.get("source", {}).get("name", "NewsAPI"),
                "published_date": datetime.fromisoformat(
                    raw_data["publishedAt"].replace("Z", "+00:00")
                ),
                "title": raw_data["title"],
                "content": raw_data.get("content", raw_data.get("description", "")),
                "url": raw_data["url"],
                "author": raw_data.get("author"),
                "categories": self._extract_categories(raw_data),
                "entities": self._extract_entities(raw_data),
                "sentiment_score": raw_data.get("sentiment", {}).get("score"),
                "relevance_score": self._calculate_relevance_score(raw_data),
                "related_games": self._extract_related_games(raw_data),
                "metadata": {
                    "source_id": raw_data.get("source", {}).get("id"),
                    "article_id": raw_data.get("id"),
                    "language": raw_data.get("language", "en"),
                    "category": raw_data.get("category"),
                    "raw_sentiment": raw_data.get("sentiment"),
                },
            }

            # Validate against schema
            return NewsData(**news_data).model_dump()

        except Exception as e:
            raise ValueError(f"Failed to transform NewsAPI data: {e}")

    def transform_gnews(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform GNews data to standardized format."""
        try:
            news_id = self._generate_news_id(raw_data["url"])

            news_data = {
                "news_id": news_id,
                "source": raw_data.get("source", {}).get("name", "GNews"),
                "published_date": datetime.fromisoformat(
                    raw_data["publishedAt"].replace("Z", "+00:00")
                ),
                "title": raw_data["title"],
                "content": raw_data.get("description", ""),
                "url": raw_data["url"],
                "author": raw_data.get("source", {}).get("name"),
                "categories": self._extract_categories(raw_data),
                "entities": self._extract_entities(raw_data),
                "sentiment_score": raw_data.get("sentiment", {}).get("score"),
                "relevance_score": self._calculate_relevance_score(raw_data),
                "related_games": self._extract_related_games(raw_data),
                "metadata": {
                    "source_id": raw_data.get("source", {}).get("id"),
                    "language": raw_data.get("language", "en"),
                    "image_url": raw_data.get("image"),
                    "raw_sentiment": raw_data.get("sentiment"),
                },
            }

            return NewsData(**news_data).model_dump()

        except Exception as e:
            raise ValueError(f"Failed to transform GNews data: {e}")

    def transform_rss(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform RSS feed data to standardized format."""
        try:
            news_id = self._generate_news_id(raw_data["link"])

            news_data = {
                "news_id": news_id,
                "source": raw_data.get("feed_name", "RSS"),
                "published_date": self._parse_rss_date(raw_data["published"]),
                "title": raw_data["title"],
                "content": raw_data.get("summary", ""),
                "url": raw_data["link"],
                "author": raw_data.get("author"),
                "categories": raw_data.get("tags", []),
                "entities": self._extract_entities(raw_data),
                "sentiment_score": None,  # RSS feeds don't typically include sentiment
                "relevance_score": self._calculate_relevance_score(raw_data),
                "related_games": self._extract_related_games(raw_data),
                "metadata": {
                    "feed_id": raw_data.get("feed_id"),
                    "guid": raw_data.get("guid"),
                    "language": raw_data.get("language", "en"),
                    "categories": raw_data.get("categories", []),
                },
            }

            return NewsData(**news_data).model_dump()

        except Exception as e:
            raise ValueError(f"Failed to transform RSS data: {e}")

    @staticmethod
    def _generate_news_id(url: str) -> str:
        """Generate unique news ID from URL."""
        return f"news_{hashlib.md5(url.encode()).hexdigest()}"

    @staticmethod
    def _extract_categories(data: Dict[str, Any]) -> List[str]:
        """Extract categories from news data."""
        categories = []

        # Add explicit categories if available
        if "category" in data:
            if isinstance(data["category"], str):
                categories.append(data["category"])
            elif isinstance(data["category"], list):
                categories.extend(data["category"])

        # Add tags if available
        if "tags" in data:
            categories.extend(data["tags"])

        return list(set(categories))  # Remove duplicates

    @staticmethod
    def _extract_entities(data: Dict[str, Any]) -> Dict[str, List[str]]:
        """Extract named entities from news content."""
        entities = {"teams": [], "players": [], "locations": [], "organizations": []}

        # Add any extracted entities from the raw data
        if "entities" in data:
            raw_entities = data["entities"]
            if isinstance(raw_entities, dict):
                for key, value in raw_entities.items():
                    if key in entities and isinstance(value, list):
                        entities[key].extend(value)

        return entities

    @staticmethod
    def _calculate_relevance_score(data: Dict[str, Any]) -> Optional[float]:
        """Calculate relevance score based on available data."""
        if "relevance" in data:
            return float(data["relevance"])
        if "score" in data:
            return float(data["score"])
        return None

    @staticmethod
    def _extract_related_games(data: Dict[str, Any]) -> Optional[List[str]]:
        """Extract related game IDs from news data."""
        if "related_games" in data:
            return data["related_games"]
        return None

    @staticmethod
    def _parse_rss_date(date_str: str) -> datetime:
        """Parse various RSS date formats."""
        try:
            # Try common RSS date formats
            formats = [
                "%a, %d %b %Y %H:%M:%S %z",  # RFC 822
                "%Y-%m-%dT%H:%M:%S%z",  # ISO 8601
                "%Y-%m-%d %H:%M:%S",  # Basic format
            ]

            for fmt in formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue

            # If no format matches, try parsing as ISO format
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))

        except Exception:
            # If all parsing fails, return current time
            return datetime.now()
