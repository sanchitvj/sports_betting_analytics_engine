from typing import Dict, Any
import time


def api_raw_cfb_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    try:
        competition = raw_data.get("competitions", [{}])[0]
        home_team = next(
            (
                team
                for team in competition.get("competitors", [])
                if team.get("homeAway") == "home"
            ),
            {},
        )
        away_team = next(
            (
                team
                for team in competition.get("competitors", [])
                if team.get("homeAway") == "away"
            ),
            {},
        )

        def get_leader_value(competition, category):
            leaders = competition.get("leaders", [])
            leader = next((l for l in leaders if l.get("name") == category), {})  # noqa: E741
            leader_stats = (
                leader.get("leaders", [{}])[0] if leader.get("leaders") else {}
            )
            return {
                "displayValue": leader_stats.get("displayValue"),
                "value": leader_stats.get("value"),
                "athlete": leader_stats.get("athlete", {}).get("displayName"),
                "team": leader_stats.get("team", {}).get("id"),
            }

        cfb_game_data = {
            "game_id": raw_data.get("id"),
            "start_time": raw_data.get("date"),
            "status_state": raw_data.get("status", {}).get("type", {}).get("state"),
            "status_detail": raw_data.get("status", {}).get("type", {}).get("detail"),
            "status_description": raw_data.get("status", {})
            .get("type", {})
            .get("description"),
            "period": raw_data.get("status", {}).get("period", 0),
            "clock": raw_data.get("status", {}).get("displayClock", "0:00"),
            # Home team
            "home_team_name": home_team.get("team", {}).get("name"),
            "home_team_id": home_team.get("team", {}).get("id"),
            "home_team_abbreviation": home_team.get("team", {}).get("abbreviation"),
            "home_team_score": int(home_team.get("score", 0)),
            "home_team_record": next(
                (
                    r.get("summary")
                    for r in home_team.get("records", [])
                    if r.get("name") == "overall"
                ),
                "0-0",
            ),
            "home_team_linescores": [
                int(ls.get("value", 0)) for ls in home_team.get("linescores", [])
            ],
            # Away team
            "away_team_name": away_team.get("team", {}).get("name"),
            "away_team_id": away_team.get("team", {}).get("id"),
            "away_team_abbreviation": away_team.get("team", {}).get("abbreviation"),
            "away_team_score": int(away_team.get("score", 0)),
            "away_team_record": next(
                (
                    r.get("summary")
                    for r in away_team.get("records", [])
                    if r.get("name") == "overall"
                ),
                "0-0",
            ),
            "away_team_linescores": [
                int(ls.get("value", 0)) for ls in away_team.get("linescores", [])
            ],
            # Game Leaders
            "passing_leader_name": get_leader_value(competition, "passingYards").get(
                "athlete"
            ),
            "passing_leader_display_value": get_leader_value(
                competition, "passingYards"
            ).get("displayValue"),
            "passing_leader_value": get_leader_value(competition, "passingYards").get(
                "value"
            ),
            "passing_leader_team": get_leader_value(competition, "passingYards").get(
                "team"
            ),
            "rushing_leader_name": get_leader_value(competition, "rushingYards").get(
                "athlete"
            ),
            "rushing_leader_display_value": get_leader_value(
                competition, "rushingYards"
            ).get("displayValue"),
            "rushing_leader_value": get_leader_value(competition, "rushingYards").get(
                "value"
            ),
            "rushing_leader_team": get_leader_value(competition, "rushingYards").get(
                "team"
            ),
            "receiving_leader_name": get_leader_value(
                competition, "receivingYards"
            ).get("athlete"),
            "receiving_leader_display_value": get_leader_value(
                competition, "receivingYards"
            ).get("displayValue"),
            "receiving_leader_value": get_leader_value(
                competition, "receivingYards"
            ).get("value"),
            "receiving_leader_team": get_leader_value(
                competition, "receivingYards"
            ).get("team"),
            # Venue
            "venue_name": competition.get("venue", {}).get("fullName"),
            "venue_city": competition.get("venue", {}).get("address", {}).get("city"),
            "venue_state": competition.get("venue", {}).get("address", {}).get("state"),
            "venue_indoor": competition.get("venue", {}).get("indoor", False),
            "broadcasts": [
                broadcast.get("names", [])[0]
                for broadcast in competition.get("broadcasts", [])
            ],
            "timestamp": int(time.time()),
        }
        return cfb_game_data  # CFBGameStats(**cfb_game_data).model_dump()

    except Exception as e:
        raise ValueError(f"Failed to transform college football data: {e}")


def api_raw_nfl_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    try:
        competition = raw_data.get("competitions", [{}])[0]
        home_team = next(
            (
                team
                for team in competition.get("competitors", [])
                if team.get("homeAway") == "home"
            ),
            {},
        )
        away_team = next(
            (
                team
                for team in competition.get("competitors", [])
                if team.get("homeAway") == "away"
            ),
            {},
        )

        # Leaders are at competition level when game is live
        def get_leader_value(competition, category):
            leaders = competition.get("leaders", [])
            leader = next((l for l in leaders if l.get("name") == category), {})
            leader_stats = (
                leader.get("leaders", [{}])[0] if leader.get("leaders") else {}
            )
            return {
                "displayValue": leader_stats.get("displayValue"),
                "value": leader_stats.get("value"),
                "athlete": leader_stats.get("athlete", {}).get("displayName"),
                "team": leader_stats.get("team", {}).get("id"),
            }

        nfl_games_data = {
            "game_id": raw_data.get("id"),
            "start_time": raw_data.get("date"),
            "status_state": raw_data.get("status", {}).get("type", {}).get("state"),
            "status_detail": raw_data.get("status", {}).get("type", {}).get("detail"),
            "status_description": raw_data.get("status", {})
            .get("type", {})
            .get("description"),
            "period": raw_data.get("status", {}).get("period", 0),
            "clock": raw_data.get("status", {}).get("displayClock", "0:00"),
            # Home team
            "home_team_name": home_team.get("team", {}).get("name"),
            "home_team_id": home_team.get("team", {}).get("id"),
            "home_team_abbreviation": home_team.get("team", {}).get("abbreviation"),
            "home_team_score": int(home_team.get("score", 0)),
            "home_team_record": next(
                (
                    r.get("summary")
                    for r in home_team.get("records", [])
                    if r.get("name") == "overall"
                ),
                "0-0",
            ),
            "home_team_linescores": [
                int(ls.get("value", 0)) for ls in home_team.get("linescores", [])
            ],
            # Away team
            "away_team_name": away_team.get("team", {}).get("name"),
            "away_team_id": away_team.get("team", {}).get("id"),
            "away_team_abbreviation": away_team.get("team", {}).get("abbreviation"),
            "away_team_score": int(away_team.get("score", 0)),
            "away_team_record": next(
                (
                    r.get("summary")
                    for r in away_team.get("records", [])
                    if r.get("name") == "overall"
                ),
                "0-0",
            ),
            "away_team_linescores": [
                int(ls.get("value", 0)) for ls in away_team.get("linescores", [])
            ],
            # Game Leaders
            "passing_leader_name": get_leader_value(competition, "passingYards").get(
                "athlete"
            ),
            "passing_leader_display_value": get_leader_value(
                competition, "passingYards"
            ).get("displayValue"),
            "passing_leader_value": get_leader_value(competition, "passingYards").get(
                "value"
            ),
            "passing_leader_team": get_leader_value(competition, "passingYards").get(
                "team"
            ),
            "rushing_leader_name": get_leader_value(competition, "rushingYards").get(
                "athlete"
            ),
            "rushing_leader_display_value": get_leader_value(
                competition, "rushingYards"
            ).get("displayValue"),
            "rushing_leader_value": get_leader_value(competition, "rushingYards").get(
                "value"
            ),
            "rushing_leader_team": get_leader_value(competition, "rushingYards").get(
                "team"
            ),
            "receiving_leader_name": get_leader_value(
                competition, "receivingYards"
            ).get("athlete"),
            "receiving_leader_display_value": get_leader_value(
                competition, "receivingYards"
            ).get("displayValue"),
            "receiving_leader_value": get_leader_value(
                competition, "receivingYards"
            ).get("value"),
            "receiving_leader_team": get_leader_value(
                competition, "receivingYards"
            ).get("team"),
            # Venue
            "venue_name": competition.get("venue", {}).get("fullName"),
            "venue_city": competition.get("venue", {}).get("address", {}).get("city"),
            "venue_state": competition.get("venue", {}).get("address", {}).get("state"),
            "venue_indoor": competition.get("venue", {}).get("indoor", False),
            "broadcasts": [
                broadcast.get("names", [])[0]
                for broadcast in competition.get("broadcasts", [])
            ],
            "timestamp": int(time.time()),
        }
        return nfl_games_data  # NFLGameStats(**nfl_games_data).model_dump()
    except Exception as e:
        raise Exception(f"Error transforming NFL game data: {e}")


def api_raw_nhl_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """Transforms raw ESPN NHL game data to defined schema format."""
    try:
        competition = raw_data.get("competitions", [{}])[0]
        home_team = next(
            (
                team
                for team in competition.get("competitors", [])
                if team.get("homeAway") == "home"
            ),
            {},
        )
        away_team = next(
            (
                team
                for team in competition.get("competitors", [])
                if team.get("homeAway") == "away"
            ),
            {},
        )

        # Get team statistics
        home_stats = home_team.get("statistics", [])
        away_stats = away_team.get("statistics", [])

        # Helper function to get stat value
        def get_stat_value(stats, name):
            stat = next((s for s in stats if s.get("name") == name), {})
            return stat.get("displayValue")

        def get_leader_value(team, category):
            leaders = team.get("leaders", [])
            leader = next((l for l in leaders if l.get("name") == category), {})
            leader_stats = (
                leader.get("leaders", [{}])[0] if leader.get("leaders") else {}
            )
            return {
                "name": leader_stats.get("athlete", {}).get("displayName"),
                "value": leader_stats.get("value"),
                # "display_value": leader_stats.get("displayValue"),
                "team": leader_stats.get("team", {}).get("id"),
            }

        nhl_game_data = {
            "game_id": raw_data.get("id"),
            "start_time": raw_data.get("date"),
            # Game status
            "status_state": raw_data.get("status", {}).get("type", {}).get("state"),
            "status_detail": raw_data.get("status", {}).get("type", {}).get("detail"),
            "status_description": raw_data.get("status", {})
            .get("type", {})
            .get("description"),
            "period": raw_data.get("status", {}).get("period", 0),
            "clock": raw_data.get("status", {}).get("displayClock", "0:00"),
            # Home team
            "home_team_name": home_team.get("team", {}).get("name"),
            "home_team_id": home_team.get("team", {}).get("id"),
            "home_team_abbreviation": home_team.get("team", {}).get("abbreviation"),
            "home_team_score": home_team.get("score"),
            # Home team statistics
            "home_team_saves": get_stat_value(home_stats, "saves"),
            "home_team_save_pct": get_stat_value(home_stats, "savePct"),
            "home_team_goals": get_stat_value(home_stats, "goals"),
            "home_team_assists": get_stat_value(home_stats, "assists"),
            "home_team_points": get_stat_value(home_stats, "points"),
            # Away team
            "away_team_name": away_team.get("team", {}).get("name"),
            "away_team_id": away_team.get("team", {}).get("id"),
            "away_team_abbreviation": away_team.get("team", {}).get("abbreviation"),
            "away_team_score": away_team.get("score"),
            # Away team statistics
            "away_team_saves": get_stat_value(away_stats, "saves"),
            "away_team_save_pct": get_stat_value(away_stats, "savePct"),
            "away_team_goals": get_stat_value(away_stats, "goals"),
            "away_team_assists": get_stat_value(away_stats, "assists"),
            "away_team_points": get_stat_value(away_stats, "points"),
            # Team records
            "home_team_record": next(
                (
                    r.get("summary")
                    for r in home_team.get("records", [])
                    if r.get("name") == "overall"
                ),
                "0-0",
            ),
            "away_team_record": next(
                (
                    r.get("summary")
                    for r in away_team.get("records", [])
                    if r.get("name") == "overall"
                ),
                "0-0",
            ),
            "home_team_linescores": [
                int(ls.get("value", 0)) for ls in home_team.get("linescores", [])
            ],
            "away_team_linescores": [
                int(ls.get("value", 0)) for ls in away_team.get("linescores", [])
            ],
            # Goals leaders
            "home_goals_leader_name": get_leader_value(home_team, "goals").get("name"),
            "home_goals_leader_value": get_leader_value(home_team, "goals").get(
                "value"
            ),
            "home_goals_leader_team": get_leader_value(home_team, "goals").get("team"),
            "away_goals_leader_name": get_leader_value(away_team, "goals").get("name"),
            "away_goals_leader_value": get_leader_value(away_team, "goals").get(
                "value"
            ),
            "away_goals_leader_team": get_leader_value(away_team, "goals").get("team"),
            # Assists leaders
            "home_assists_leader_name": get_leader_value(home_team, "assists").get(
                "name"
            ),
            "home_assists_leader_value": get_leader_value(home_team, "assists").get(
                "value"
            ),
            "home_assists_leader_team": get_leader_value(home_team, "assists").get(
                "team"
            ),
            "away_assists_leader_name": get_leader_value(away_team, "assists").get(
                "name"
            ),
            "away_assists_leader_value": get_leader_value(away_team, "assists").get(
                "value"
            ),
            "away_assists_leader_team": get_leader_value(away_team, "assists").get(
                "team"
            ),
            # Points leaders
            "home_points_leader_name": get_leader_value(home_team, "points").get(
                "name"
            ),
            "home_points_leader_value": get_leader_value(home_team, "points").get(
                "value"
            ),
            "home_points_leader_team": get_leader_value(home_team, "points").get(
                "team"
            ),
            "away_points_leader_name": get_leader_value(away_team, "points").get(
                "name"
            ),
            "away_points_leader_value": get_leader_value(away_team, "points").get(
                "value"
            ),
            "away_points_leader_team": get_leader_value(away_team, "points").get(
                "team"
            ),
            # Venue information
            "venue_name": competition.get("venue", {}).get("fullName"),
            "venue_city": competition.get("venue", {}).get("address", {}).get("city"),
            "venue_state": competition.get("venue", {}).get("address", {}).get("state"),
            "venue_indoor": competition.get("venue", {}).get("indoor", True),
            # Broadcasts and timestamp
            "broadcasts": [
                broadcast.get("names", [])[0]
                for broadcast in competition.get("broadcasts", [])
            ],
            "timestamp": int(time.time()),
        }
        return nhl_game_data  # NHLGameStats(**nhl_game_data).model_dump()

    except Exception as e:
        raise ValueError(f"Failed to transform NHL game data: {e}")


def api_raw_nba_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """Transforms raw ESPN game data to defined schema format."""
    try:
        competition = raw_data.get("competitions", [{}])[0]
        home_team = next(
            (
                team
                for team in competition.get("competitors", [])
                if team.get("homeAway") == "home"
            ),
            {},
        )
        away_team = next(
            (
                team
                for team in competition.get("competitors", [])
                if team.get("homeAway") == "away"
            ),
            {},
        )

        # Get team statistics
        home_stats = home_team.get("statistics", [])
        away_stats = away_team.get("statistics", [])

        # Helper function to get stat value
        def get_stat_value(stats, name):
            stat = next((s for s in stats if s.get("name") == name), {})
            return stat.get("displayValue")

        def get_leader_value(team, category):
            leaders = team.get("leaders", [])
            leader = next((l for l in leaders if l.get("name") == category), {})
            leader_stats = (
                leader.get("leaders", [{}])[0] if leader.get("leaders") else {}
            )
            return {
                "name": leader_stats.get("athlete", {}).get("displayName"),
                "value": leader_stats.get("value"),
                "display_value": leader_stats.get("displayValue"),
                "team": leader_stats.get("team", {}).get("id"),
            }

        nba_game_data = {
            "game_id": raw_data.get("id"),
            "start_time": raw_data.get("date"),
            # game status
            "status_state": raw_data.get("status", {}).get("type", {}).get("state"),
            "status_detail": raw_data.get("status", {}).get("type", {}).get("detail"),
            "status_description": raw_data.get("status", {})
            .get("type", {})
            .get("description"),
            "period": raw_data.get("status", {}).get("period", 0),
            "clock": raw_data.get("status", {}).get("displayClock", "0:00"),
            # home team
            "home_team_name": home_team.get("team", {}).get("name"),
            "home_team_id": home_team.get("team", {}).get("id"),
            "home_team_abbreviation": home_team.get("team", {}).get("abbreviation"),
            "home_team_score": home_team.get("score"),
            # Home team statistics
            "home_team_field_goals": get_stat_value(home_stats, "fieldGoalPct"),
            "home_team_three_pointers": get_stat_value(home_stats, "threePointPct"),
            "home_team_free_throws": get_stat_value(home_stats, "freeThrowPct"),
            "home_team_rebounds": get_stat_value(home_stats, "rebounds"),
            "home_team_assists": get_stat_value(home_stats, "assists"),
            # away team
            "away_team_name": away_team.get("team", {}).get("name"),
            "away_team_id": away_team.get("team", {}).get("id"),
            "away_team_abbreviation": away_team.get("team", {}).get("abbreviation"),
            "away_team_score": away_team.get("score"),
            # Away team statistics
            "away_team_field_goals": get_stat_value(away_stats, "fieldGoalPct"),
            "away_team_three_pointers": get_stat_value(away_stats, "threePointPct"),
            "away_team_free_throws": get_stat_value(away_stats, "freeThrowPct"),
            "away_team_rebounds": get_stat_value(away_stats, "rebounds"),
            "away_team_assists": get_stat_value(away_stats, "assists"),
            # Team records
            "home_team_record": next(
                (
                    r.get("summary")
                    for r in home_team.get("records", [])
                    if r.get("name") == "overall"
                ),
                "0-0",
            ),
            "away_team_record": next(
                (
                    r.get("summary")
                    for r in away_team.get("records", [])
                    if r.get("name") == "overall"
                ),
                "0-0",
            ),
            "home_team_linescores": [
                int(ls.get("value", 0)) for ls in home_team.get("linescores", [])
            ],
            "away_team_linescores": [
                int(ls.get("value", 0)) for ls in away_team.get("linescores", [])
            ],
            # points leader
            "home_points_leader_name": get_leader_value(home_team, "points").get(
                "name"
            ),
            "home_points_leader_value": get_leader_value(home_team, "points").get(
                "value"
            ),
            "home_points_leader_team": get_leader_value(home_team, "points").get(
                "team"
            ),
            "away_points_leader_name": get_leader_value(away_team, "points").get(
                "name"
            ),
            "away_points_leader_value": get_leader_value(away_team, "points").get(
                "value"
            ),
            "away_points_leader_team": get_leader_value(away_team, "points").get(
                "team"
            ),
            # rebounds leader
            "home_rebounds_leader_name": get_leader_value(home_team, "rebounds").get(
                "name"
            ),
            "home_rebounds_leader_value": get_leader_value(home_team, "rebounds").get(
                "value"
            ),
            "home_rebounds_leader_team": get_leader_value(home_team, "rebounds").get(
                "team"
            ),
            "away_rebounds_leader_name": get_leader_value(away_team, "rebounds").get(
                "name"
            ),
            "away_rebounds_leader_value": get_leader_value(away_team, "rebounds").get(
                "value"
            ),
            "away_rebounds_leader_team": get_leader_value(away_team, "rebounds").get(
                "team"
            ),
            # assists leader
            "home_assists_leader_name": get_leader_value(home_team, "assists").get(
                "name"
            ),
            "home_assists_leader_value": get_leader_value(home_team, "assists").get(
                "value"
            ),
            "away_assists_leader_name": get_leader_value(away_team, "assists").get(
                "name"
            ),
            "away_assists_leader_value": get_leader_value(away_team, "assists").get(
                "value"
            ),
            "home_assists_leader_team": get_leader_value(home_team, "assists").get(
                "team"
            ),
            "away_assists_leader_team": get_leader_value(away_team, "assists").get(
                "team"
            ),
            # rating leader
            "home_rating_leader_name": get_leader_value(home_team, "rating").get(
                "name"
            ),
            "home_rating_leader_value": get_leader_value(home_team, "rating").get(
                "value"
            ),
            "home_rating_leader_display_value": get_leader_value(
                home_team, "rating"
            ).get("display_value"),
            "home_rating_leader_team": get_leader_value(home_team, "rating").get(
                "team"
            ),
            "away_rating_leader_name": get_leader_value(away_team, "rating").get(
                "name"
            ),
            "away_rating_leader_value": get_leader_value(away_team, "rating").get(
                "value"
            ),
            "away_rating_leader_team": get_leader_value(away_team, "rating").get(
                "team"
            ),
            "away_rating_leader_display_value": get_leader_value(
                away_team, "rating"
            ).get("display_value"),
            # venue
            "venue_name": competition.get("venue", {}).get("fullName"),
            "venue_city": competition.get("venue", {}).get("address", {}).get("city"),
            "venue_state": competition.get("venue", {}).get("address", {}).get("state"),
            "broadcasts": [
                broadcast.get("names", [])[0]
                for broadcast in competition.get("broadcasts", [])
            ],
            "timestamp": int(time.time()),
        }
        return nba_game_data  # NBAGameStats(**nba_game_data).model_dump()

    except Exception as e:
        raise Exception(f"Error transforming game data: {e}")
