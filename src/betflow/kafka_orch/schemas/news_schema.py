from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime
from typing import List, Optional, Dict


class NewsData(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    news_id: str
    source: str
    published_date: datetime
    title: str
    content: str
    url: str
    author: Optional[str]
    categories: List[str]
    entities: Dict[str, List[str]] = Field(
        description="Extracted entities like teams, players, etc."
    )
    sentiment_score: Optional[float]
    relevance_score: Optional[float]
    related_games: Optional[List[str]]
    metadata: Optional[Dict[str, any]]
