import time
import requests
from typing import Dict, Optional, List


class SportsHistoricalConnector:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.request_count = 0
        self.start_time = time.time()
        self.MAX_REQUESTS_PER_MINUTE = 10
        self.urls = {
            "nba": "https://v2.nba.api-sports.io/games",
            "nfl": "https://v1.american-football.api-sports.io/games",
            "ncaa": "https://v1.american-football.api-sports.io/games",
            "nhl": "https://v1.hockey.api-sports.io/games",
        }
        self.league_ids = {"nba": "standard", "nfl": "1", "ncaa": "2", "nhl": "57"}

    def _check_rate_limit(self):
        """Implement rate limiting"""
        if self.request_count >= self.MAX_REQUESTS_PER_MINUTE:
            elapsed_time = time.time() - self.start_time
            if elapsed_time < 60:
                time.sleep(60 - elapsed_time)
            self.request_count = 0
            self.start_time = time.time()

    def _get_headers(self, sport: str) -> Dict[str, str]:
        """Generate headers based on sport"""
        host = (
            "v2.nba.api-sports.io"
            if sport == "nba"
            else {
                "nfl": "v1.american-football.api-sports.io",
                "ncaa": "v1.american-football.api-sports.io",
                "nhl": "v1.hockey.api-sports.io",
            }[sport]
        )

        return {"x-rapidapi-host": host, "x-rapidapi-key": self.api_key}

    def fetch_games(self, sport: str, season: str) -> Optional[Dict]:
        """
        Fetch games for a specific sport and season

        Args:
            sport: One of 'nba', 'nfl', 'ncaa', 'nhl'
            season: Season year (e.g., '2022')
        """
        self._check_rate_limit()

        base_url = self.urls.get(sport)
        league_id = self.league_ids.get(sport)

        if not base_url or not league_id:
            raise ValueError(f"Invalid sport: {sport}")

        url = f"{base_url}?league={league_id}&season={season}"
        headers = self._get_headers(sport)

        try:
            response = requests.get(url, headers=headers)
            self.request_count += 1

            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(f"Failed to fetch data: {response.status_code}")

        except requests.exceptions.RequestException as e:
            raise Exception(f"Request failed: {str(e)}")


class NHLDataFetcher:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://v1.hockey.api-sports.io"
        self.headers = {
            "x-rapidapi-host": "v1.hockey.api-sports.io",
            "x-rapidapi-key": api_key,
        }
        self.request_count = 0
        self.last_request_time = time.time()

    def _rate_limit(self):
        """Implement rate limiting - 10 requests per minute"""
        current_time = time.time()
        if self.request_count >= 10:
            elapsed = current_time - self.last_request_time
            if elapsed < 60:
                time.sleep(60 - elapsed)
            self.request_count = 0
            self.last_request_time = time.time()

    def fetch_season_games(self, season: str) -> List[Dict]:
        """Fetch all games for a season"""
        self._rate_limit()
        url = f"{self.base_url}/games"
        params = {
            "league": "57",  # NHL league ID
            "season": season,
        }
        response = requests.get(url, headers=self.headers, params=params)
        self.request_count += 1
        return response.json()["response"]

    def fetch_game_details(self, game_id: str) -> Dict:
        """Fetch detailed information for a specific game"""
        self._rate_limit()
        url = f"{self.base_url}/games"
        params = {"id": game_id}
        response = requests.get(url, headers=self.headers, params=params)
        self.request_count += 1
        return response.json()["response"][0]

    def fetch_game_events(self, game_id: str) -> List[Dict]:
        """Fetch events for a specific game"""
        self._rate_limit()
        url = f"{self.base_url}/games/events"
        params = {"game": game_id}
        response = requests.get(url, headers=self.headers, params=params)
        self.request_count += 1
        return response.json()["response"]

    def fetch_complete_game_data(self, game_id: str) -> Dict:
        """Fetch and combine all data for a game"""
        game_details = self.fetch_game_details(game_id)
        game_events = self.fetch_game_events(game_id)

        # Calculate assists
        home_team_id = game_details["teams"]["home"]["id"]
        away_team_id = game_details["teams"]["away"]["id"]

        home_assists = sum(
            len(event["assists"])
            for event in game_events
            if event["team"]["id"] == home_team_id
        )
        away_assists = sum(
            len(event["assists"])
            for event in game_events
            if event["team"]["id"] == away_team_id
        )

        return {
            "game_id": game_details["id"],
            "date": game_details["date"],
            "time": game_details["time"],
            "week": game_details["week"],
            "status": game_details["status"]["long"],
            "league_id": game_details["league"]["id"],
            "season": game_details["league"]["season"],
            "home_team_name": game_details["teams"]["home"]["name"],
            "away_team_name": game_details["teams"]["away"]["name"],
            "home_team_id": home_team_id,
            "away_team_id": away_team_id,
            "home_score": game_details["scores"]["home"],
            "away_score": game_details["scores"]["away"],
            "home_assists": home_assists,
            "away_assists": away_assists,
            "home_points": (game_details["scores"]["home"] or 0) + home_assists,
            "away_points": (game_details["scores"]["away"] or 0) + away_assists,
            "periods": game_details["periods"],
        }


if __name__ == "__main__":
    fetcher = NHLDataFetcher(api_key="your_api_key")

    # Fetch all games for 2022 season
    season_games = fetcher.fetch_season_games("2022")

    # Get complete data for first game
    game_id = season_games[0]["id"]
    complete_game_data = fetcher.fetch_complete_game_data(str(game_id))
