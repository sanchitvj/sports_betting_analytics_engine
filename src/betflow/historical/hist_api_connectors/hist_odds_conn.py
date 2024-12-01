from typing import Dict, List
import asyncio
import aiohttp
from datetime import datetime, timedelta
from collections import deque
import os
# from betflow.api_connectors.historical import NBAHistoricalConnector


class HistoricalOddsConnector:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.the-odds-api.com/v4/historical/sports"
        self.sport_keys = {
            "nfl": "americanfootball_nfl",
            "ncaa": "americanfootball_ncaaf",
            "nba": "basketball_nba",
            "nhl": "hockey_nhl",
        }
        self.request_timestamps = deque(maxlen=300)

    async def _rate_limit(self):
        """Ensure no more than 300 requests per minute"""
        now = datetime.now()
        if len(self.request_timestamps) == 300:
            elapsed = (now - self.request_timestamps[0]).total_seconds()
            if elapsed < 60:
                await asyncio.sleep(60 - elapsed)
        self.request_timestamps.append(now)

    def generate_game_timestamps(
        self, game_start: str, game_duration: int = 180
    ) -> List[str]:
        """Generate 30-min interval timestamps during game"""
        timestamps = []
        start_time = datetime.fromisoformat(game_start.replace("Z", "+00:00"))
        end_time = start_time + timedelta(minutes=game_duration)

        current = start_time
        while current <= end_time:
            timestamps.append(current.strftime("%Y-%m-%dT%H:%M:%SZ"))
            current += timedelta(minutes=30)

        return timestamps

    async def fetch_odds_snapshot(
        self, session, sport: str, game_id: str, timestamp: str
    ) -> Dict:
        """Fetch odds snapshot for a specific game and time"""
        await self._rate_limit()
        url = f"{self.base_url}/{self.sport_keys[sport]}/events/{game_id}/odds"
        params = {
            "apiKey": self.api_key,
            "regions": "us",
            "markets": "h2h",
            "date": timestamp,
        }

        async with session.get(url, params=params) as response:
            return await response.json()

    async def process_game_odds(self, session, sport: str, game: Dict) -> List[Dict]:
        """Process odds for a single game"""
        game_id = game["id"]
        commence_time = game["commence_time"]
        game_duration = 180  # 3 hours default

        # Adjust duration based on sport and if overtime
        if sport in ["nfl", "ncaa"]:
            game_duration = 240  # 4 hours for football
        elif "overtime" in game.get("scores", {}):
            game_duration += 30  # Add 30 mins for overtime

        timestamps = self.generate_game_timestamps(commence_time, game_duration)
        game_odds_history = []

        for timestamp in timestamps:
            try:
                odds_data = await self.fetch_odds_snapshot(
                    session, sport, game_id, timestamp
                )
                if odds_data:
                    game_odds_history.append(
                        {
                            "game_id": game_id,
                            "timestamp": timestamp,
                            "odds_snapshot": odds_data,
                        }
                    )
            except Exception as e:
                print(
                    f"Error fetching odds for game {game_id} at {timestamp}: {str(e)}"
                )

        return game_odds_history

    async def fetch_season_odds(
        self, sport: str, games: List[Dict], batch_size: int = 5
    ):
        """Fetch historical odds for a season's games"""
        async with aiohttp.ClientSession() as session:
            all_odds_data = []

            for i in range(0, len(games), batch_size):
                batch = games[i : i + batch_size]
                tasks = [self.process_game_odds(session, sport, game) for game in batch]

                try:
                    batch_results = await asyncio.gather(*tasks)
                    for game_odds in batch_results:
                        all_odds_data.extend(game_odds)
                except Exception as e:
                    print(f"Error processing batch: {str(e)}")

            return all_odds_data


async def main():
    games_connector = NBAHistoricalConnector(api_key=os.getenv("API_SPORTS_IO_KEY"))
    odds_connector = HistoricalOddsConnector(api_key=os.getenv("ODDS_API_KEY"))

    # Fetch games first
    async with aiohttp.ClientSession() as session:
        nfl_games = await games_connector.fetch_season_games(session, "nfl", "2024")

        # Then fetch odds for those games
        nfl_odds = await odds_connector.fetch_season_odds("nfl", nfl_games)

        return nfl_odds


if __name__ == "__main__":
    odds_data = asyncio.run(main())
