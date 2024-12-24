from abc import ABC, abstractmethod
import asyncio
from collections import deque
from datetime import datetime
from typing import Dict, List, Union

import os
from dotenv import load_dotenv

import aiohttp

load_dotenv("my.env")


class BaseHistoricalConnector(ABC):
    def __init__(self, api_key: str, base_url: str, host: str):
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {"x-rapidapi-host": host, "x-rapidapi-key": api_key}
        self.request_timestamps = deque(maxlen=300)  # 300 requests/minute limit

    async def _rate_limit(self):
        """Ensure no more than 300 requests per minute"""
        now = datetime.now()
        if len(self.request_timestamps) == 300:
            elapsed = (now - self.request_timestamps[0]).total_seconds()
            if elapsed < 60:
                await asyncio.sleep(60 - elapsed)
        self.request_timestamps.append(now)

    async def _fetch_data(self, session, endpoint: str, params: Dict) -> Dict:
        """Generic fetch method with rate limiting"""
        await self._rate_limit()
        url = f"{self.base_url}/{endpoint}"
        async with session.get(url, headers=self.headers, params=params) as response:
            return await response.json()

    async def fetch_games_by_date(
        self, session, league: Union[int, str], season: int, date_str: str
    ) -> List[Dict]:
        """Fetch games for a specific date"""
        try:
            return await self._fetch_data(
                session, "games", {"league": league, "season": season, "date": date_str}
            )
        except Exception as e:
            print(f"Error fetching games for date {date_str}: {str(e)}")
            raise

    @abstractmethod
    async def fetch_season_games(self, session, season: str) -> List[Dict]:
        pass

    @abstractmethod
    async def fetch_game_statistics(self, session, game_id: str) -> Dict:
        pass

    @abstractmethod
    def process_game_data(self, game: Dict, stats: Dict) -> Dict:
        pass


class NBAHistoricalConnector(BaseHistoricalConnector):
    def __init__(self, api_key: str):
        super().__init__(
            api_key=api_key,
            base_url="https://v2.nba.api-sports.io",
            host="v2.nba.api-sports.io",
        )

    async def fetch_season_games(self, session, season: str) -> dict:
        """Fetch all games for a season"""
        return await self._fetch_data(
            session, "games", {"league": "standard", "season": season}
        )

    async def fetch_game_statistics(self, session, game_id: str) -> Dict:
        """Fetch statistics for a specific game"""
        return await self._fetch_data(session, "games/statistics", {"id": game_id})

    def process_game_data(self, game: Dict, stats: Dict) -> Dict:
        """Process and combine game data"""
        home_stats = next(
            item
            for item in stats["response"]
            if item["team"]["id"] == game["teams"]["home"]["id"]
        )
        away_stats = next(
            item
            for item in stats["response"]
            if item["team"]["id"] == game["teams"]["visitors"]["id"]
        )

        return {
            "game_id": game["id"],
            "date": game["date"]["start"],
            "time": game["date"]["duration"],
            "stage": game["stage"],
            "status": game["status"]["long"],
            "arena": {
                "name": game["arena"]["name"],
                "city": game["arena"]["city"],
                "state": game["arena"]["state"],
            },
            "teams": {
                "home": {
                    "id": game["teams"]["home"]["id"],
                    "name": game["teams"]["home"]["name"],
                    "nickname": game["teams"]["home"]["nickname"],
                    "code": game["teams"]["home"]["code"],
                    "scores": {
                        "points": game["scores"]["home"]["points"],
                        "linescore": game["scores"]["home"]["linescore"],
                        "win": game["scores"]["home"]["win"],
                        "loss": game["scores"]["home"]["loss"],
                    },
                    "statistics": {
                        "fastBreakPoints": home_stats["statistics"][0][
                            "fastBreakPoints"
                        ],
                        "pointsInPaint": home_stats["statistics"][0]["pointsInPaint"],
                        "biggestLead": home_stats["statistics"][0]["biggestLead"],
                        "pointsOffTurnovers": home_stats["statistics"][0][
                            "pointsOffTurnovers"
                        ],
                        "points": home_stats["statistics"][0]["points"],
                        "fgm": home_stats["statistics"][0]["fgm"],
                        "fga": home_stats["statistics"][0]["fga"],
                        "fgp": home_stats["statistics"][0]["fgp"],
                        "ftm": home_stats["statistics"][0]["ftm"],
                        "fta": home_stats["statistics"][0]["fta"],
                        "ftp": home_stats["statistics"][0]["ftp"],
                        "tpm": home_stats["statistics"][0]["tpm"],
                        "tpa": home_stats["statistics"][0]["tpa"],
                        "tpp": home_stats["statistics"][0]["tpp"],
                        "offReb": home_stats["statistics"][0]["offReb"],
                        "defReb": home_stats["statistics"][0]["defReb"],
                        "totReb": home_stats["statistics"][0]["totReb"],
                        "assists": home_stats["statistics"][0]["assists"],
                        "pFouls": home_stats["statistics"][0]["pFouls"],
                        "steals": home_stats["statistics"][0]["steals"],
                        "turnovers": home_stats["statistics"][0]["turnovers"],
                        "blocks": home_stats["statistics"][0]["blocks"],
                        "plusMinus": home_stats["statistics"][0]["plusMinus"],
                    },
                },
                "away": {
                    "id": game["teams"]["visitors"]["id"],
                    "name": game["teams"]["visitors"]["name"],
                    "nickname": game["teams"]["visitors"]["nickname"],
                    "code": game["teams"]["visitors"]["code"],
                    "scores": {
                        "points": game["scores"]["visitors"]["points"],
                        "linescore": game["scores"]["visitors"]["linescore"],
                        "win": game["scores"]["visitors"]["win"],
                        "loss": game["scores"]["visitors"]["loss"],
                    },
                    "statistics": {
                        "fastBreakPoints": away_stats["statistics"][0][
                            "fastBreakPoints"
                        ],
                        "pointsInPaint": away_stats["statistics"][0]["pointsInPaint"],
                        "biggestLead": away_stats["statistics"][0]["biggestLead"],
                        "pointsOffTurnovers": away_stats["statistics"][0][
                            "pointsOffTurnovers"
                        ],
                        "points": away_stats["statistics"][0]["points"],
                        "fgm": away_stats["statistics"][0]["fgm"],
                        "fga": away_stats["statistics"][0]["fga"],
                        "fgp": away_stats["statistics"][0]["fgp"],
                        "ftm": away_stats["statistics"][0]["ftm"],
                        "fta": away_stats["statistics"][0]["fta"],
                        "ftp": away_stats["statistics"][0]["ftp"],
                        "tpm": away_stats["statistics"][0]["tpm"],
                        "tpa": away_stats["statistics"][0]["tpa"],
                        "tpp": away_stats["statistics"][0]["tpp"],
                        "offReb": away_stats["statistics"][0]["offReb"],
                        "defReb": away_stats["statistics"][0]["defReb"],
                        "totReb": away_stats["statistics"][0]["totReb"],
                        "assists": away_stats["statistics"][0]["assists"],
                        "pFouls": away_stats["statistics"][0]["pFouls"],
                        "steals": away_stats["statistics"][0]["steals"],
                        "turnovers": away_stats["statistics"][0]["turnovers"],
                        "blocks": away_stats["statistics"][0]["blocks"],
                        "plusMinus": away_stats["statistics"][0]["plusMinus"],
                    },
                },
            },
            "periods": game["periods"],
            "officials": game["officials"],
            "timesTied": game["timesTied"],
            "leadChanges": game["leadChanges"],
        }


class NHLHistoricalConnector(BaseHistoricalConnector):
    def __init__(self, api_key: str):
        super().__init__(
            api_key=api_key,
            base_url="https://v1.hockey.api-sports.io",
            host="v1.hockey.api-sports.io",
        )

    async def fetch_season_games(self, session, season: str) -> dict:
        """Fetch all games for a season"""
        return await self._fetch_data(
            session, "games", {"league": "57", "season": season}
        )

    async def fetch_game_events(self, session, game_id: str) -> Dict:
        """Fetch events for a specific game"""
        return await self._fetch_data(session, "games/events", {"game": game_id})

    def calculate_assists(self, events: List[Dict], team_id: int) -> int:
        """Calculate total assists for a team in a game"""
        total_assists = sum(
            len(event["assists"])
            for event in events
            if event["team"]["id"] == team_id and event["type"] == "goal"
        )
        return total_assists

    def process_game_data(self, game: Dict, events: Dict) -> Dict:
        """Process and combine game data with events"""
        home_team_id = game["teams"]["home"]["id"]
        away_team_id = game["teams"]["away"]["id"]

        # Calculate assists for both teams
        events_list = events.get("response", [])
        home_assists = self.calculate_assists(events_list, home_team_id)
        away_assists = self.calculate_assists(events_list, away_team_id)

        return {
            "game_id": game["id"],
            "date": game["date"],
            "time": game["time"],
            "timezone": game["timezone"],
            "week": game["week"],
            "status": game["status"]["long"],
            "league": {
                "id": game["league"]["id"],
                "name": game["league"]["name"],
                "type": game["league"]["type"],
                "season": game["league"]["season"],
            },
            "teams": {
                "home": {
                    "id": home_team_id,
                    "name": game["teams"]["home"]["name"],
                    "score": game["scores"]["home"],
                    "assists": home_assists,
                    "total_points": game["scores"]["home"] + home_assists,
                },
                "away": {
                    "id": away_team_id,
                    "name": game["teams"]["away"]["name"],
                    "score": game["scores"]["away"],
                    "assists": away_assists,
                    "total_points": game["scores"]["away"] + away_assists,
                },
            },
            "periods": {
                "first": game["periods"]["first"],
                "second": game["periods"]["second"],
                "third": game["periods"]["third"],
                "overtime": game["periods"]["overtime"],
                "penalties": game["periods"]["penalties"],
            },
            "country": {
                "id": game["country"]["id"],
                "name": game["country"]["name"],
                "code": game["country"]["code"],
            },
        }

    async def fetch_batch(self, session, game_ids: List[str], batch_size: int = 50):
        """Fetch data for multiple games in batches"""
        all_game_data = []

        for i in range(0, len(game_ids), batch_size):
            batch = game_ids[i : i + batch_size]
            tasks = []

            for game_id in batch:
                tasks.append(self.fetch_game_events(session, game_id))

            try:
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)

                for j, events in enumerate(batch_results):
                    if isinstance(events, Exception):
                        print(f"Error fetching game {batch[j]}: {str(events)}")
                        continue

                    if events and "response" in events:
                        game_data = await self._fetch_data(
                            session, "games", {"id": batch[j]}
                        )
                        if game_data and "response" in game_data:
                            processed_data = self.process_game_data(
                                game_data["response"][0], events
                            )
                            all_game_data.append(processed_data)

            except Exception as e:
                print(f"Batch processing error: {str(e)}")

        return all_game_data


class FootballHistoricalConnector(BaseHistoricalConnector):
    def __init__(self, api_key: str, league_id: str):
        super().__init__(
            api_key=api_key,
            base_url="https://v1.american-football.api-sports.io",
            host="v1.american-football.api-sports.io",
        )
        self.league_id = league_id  # "1" for NFL, "2" for NCAA

    async def fetch_season_games(self, session, season: str) -> dict:
        """Fetch all games for a season"""
        return await self._fetch_data(
            session, "games", {"league": self.league_id, "season": season}
        )

    async def fetch_game_statistics(self, session, game_id: str) -> Dict:
        """Fetch statistics for a specific game"""
        return await self._fetch_data(
            session, "games/statistics/teams", {"id": game_id}
        )

    def process_game_data(self, game: Dict, stats: Dict) -> Dict:
        """Process and combine game data with statistics"""
        home_stats = next(
            item
            for item in stats["response"]
            if item["team"]["id"] == game["teams"]["home"]["id"]
        )
        away_stats = next(
            item
            for item in stats["response"]
            if item["team"]["id"] == game["teams"]["away"]["id"]
        )

        return {
            "game_id": game["game"]["id"],
            "stage": game["game"]["stage"],
            "week": game["game"]["week"],
            "date": {
                "date": game["game"]["date"]["date"],
                "time": game["game"]["date"]["time"],
                "timezone": game["game"]["date"]["timezone"],
                "timestamp": game["game"]["date"]["timestamp"],
            },
            "venue": {
                "name": game["game"]["venue"]["name"],
                "city": game["game"]["venue"]["city"],
            },
            "status": {
                "short": game["game"]["status"]["short"],
                "long": game["game"]["status"]["long"],
            },
            "league": {
                "id": game["league"]["id"],
                "name": game["league"]["name"],
                "season": game["league"]["season"],
            },
            "teams": {
                "home": {
                    "id": game["teams"]["home"]["id"],
                    "name": game["teams"]["home"]["name"],
                    "scores": game["scores"]["home"],
                    "statistics": {
                        "first_downs": home_stats["statistics"]["first_downs"],
                        "plays": home_stats["statistics"]["plays"],
                        "yards": home_stats["statistics"]["yards"],
                        "passing": home_stats["statistics"]["passing"],
                        "rushings": home_stats["statistics"]["rushings"],
                        "red_zone": home_stats["statistics"]["red_zone"],
                        "penalties": home_stats["statistics"]["penalties"],
                        "turnovers": home_stats["statistics"]["turnovers"],
                        "posession": home_stats["statistics"]["posession"],
                        "interceptions": home_stats["statistics"]["interceptions"],
                        "fumbles_recovered": home_stats["statistics"][
                            "fumbles_recovered"
                        ],
                        "sacks": home_stats["statistics"]["sacks"],
                        "safeties": home_stats["statistics"]["safeties"],
                        "points_against": home_stats["statistics"]["points_against"],
                    },
                },
                "away": {
                    "id": game["teams"]["away"]["id"],
                    "name": game["teams"]["away"]["name"],
                    "scores": game["scores"]["away"],
                    "statistics": {
                        "first_downs": away_stats["statistics"]["first_downs"],
                        "plays": away_stats["statistics"]["plays"],
                        "yards": away_stats["statistics"]["yards"],
                        "passing": away_stats["statistics"]["passing"],
                        "rushings": away_stats["statistics"]["rushings"],
                        "red_zone": away_stats["statistics"]["red_zone"],
                        "penalties": away_stats["statistics"]["penalties"],
                        "turnovers": away_stats["statistics"]["turnovers"],
                        "posession": away_stats["statistics"]["posession"],
                        "interceptions": away_stats["statistics"]["interceptions"],
                        "fumbles_recovered": away_stats["statistics"][
                            "fumbles_recovered"
                        ],
                        "sacks": away_stats["statistics"]["sacks"],
                        "safeties": away_stats["statistics"]["safeties"],
                        "points_against": away_stats["statistics"]["points_against"],
                    },
                },
            },
        }


async def fetch_historical_data():
    api_key = os.getenv("API_SPORTS_IO_KEY")
    season = "2022"

    # Initialize connectors
    nba_connector = NBAHistoricalConnector(api_key)
    nhl_connector = NHLHistoricalConnector(api_key)
    nfl_connector = FootballHistoricalConnector(api_key, "1")  # NFL
    ncaa_connector = FootballHistoricalConnector(api_key, "2")  # NCAA

    async with aiohttp.ClientSession() as session:
        # Fetch data for each sport
        tasks = [
            fetch_season_data(nba_connector, session, season),
            fetch_season_data(nhl_connector, session, season),
            fetch_season_data(nfl_connector, session, season),
            fetch_season_data(ncaa_connector, session, season),
        ]

        nba_data, nhl_data, nfl_data, ncaa_data = await asyncio.gather(*tasks)
        return nba_data, nhl_data, nfl_data, ncaa_data


async def fetch_season_data(connector, session, season: str):
    """Generic function to fetch season data for any sport"""
    # Get all games for the season
    season_games = await connector.fetch_season_games(session, season)

    # Process in batches of 50
    batch_size = 50
    all_games_data = []

    for i in range(0, len(season_games), batch_size):
        batch = season_games[i : i + batch_size]
        tasks = []

        for game in batch:
            game_id = str(game["id"])
            if isinstance(connector, NHLHistoricalConnector):
                tasks.append(connector.fetch_game_events(session, game_id))
            else:
                tasks.append(connector.fetch_game_statistics(session, game_id))

        batch_results = await asyncio.gather(*tasks, return_exceptions=True)

        for game, stats in zip(batch, batch_results):
            if not isinstance(stats, Exception):
                processed_game = connector.process_game_data(game, stats)
                all_games_data.append(processed_game)

    return all_games_data


if __name__ == "__main__":
    nba_data, nhl_data, nfl_data, ncaa_data = asyncio.run(fetch_historical_data())
