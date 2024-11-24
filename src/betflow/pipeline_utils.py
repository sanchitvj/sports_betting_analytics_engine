from betflow.api_connectors.games_conn import ESPNConnector
from betflow.api_connectors.odds_conn import OddsAPIConnector
from datetime import datetime


def get_live_games(espn_connector: ESPNConnector, sport: str, league: str) -> list:
    """Get live or upcoming NBA games."""
    try:
        endpoint = f"{sport}/{league}/scoreboard"
        raw_data = espn_connector.make_request(endpoint)

        games_list = []
        for game in raw_data.get("events", []):
            competition = game.get("competitions", [{}])[0]
            venue = competition.get("venue", {})

            # Get teams
            competitors = competition.get("competitors", [])
            home_team = next(
                (team for team in competitors if team.get("homeAway") == "home"), {}
            )
            away_team = next(
                (team for team in competitors if team.get("homeAway") == "away"), {}
            )

            game_info = {
                "game_id": game.get("id"),
                "home_team": home_team.get("team", {}).get("name"),
                "away_team": away_team.get("team", {}).get("name"),
                "venue_id": venue.get("id"),
                "venue_name": venue.get("fullName"),
                "league": league,
                "status": game.get("status", {}).get("type", {}).get("state"),
                "start_time": game.get("date"),
            }
            if game_info["status"] == "in":
                games_list.append(game_info)

        return games_list
    except Exception as e:
        raise Exception(f"Failed to get live games: {e}")


def get_live_odds(odds_connector: OddsAPIConnector, sport: str) -> list:
    """Get live odds for a sport."""
    try:
        raw_data = odds_connector.make_request(f"sports/{sport}/odds")
        odds_list = []
        for game in raw_data:
            odds_info = {
                "game_id": game.get("id"),
                "sport_key": game.get("sport_key"),
                "sport_title": game.get("sport_title"),
                "commence_time": game.get("commence_time"),
                "home_team": game.get("home_team"),
                "away_team": game.get("away_team"),
                "bookmakers": game.get("bookmakers", []),
                "timestamp": int(datetime.now().timestamp()),
            }
            odds_list.append(odds_info)

        return odds_list
    except Exception as e:
        raise Exception(f"Failed to get odds data: {e}")
