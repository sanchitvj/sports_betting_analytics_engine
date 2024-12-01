SELECT
  CONCAT(home_team_name, ' vs ', away_team_name) as game,
  latest(home_team_score) as "Home Score",
  latest(away_team_score) as "Away Score"
FROM nhl_games_analytics
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '60' MINUTE
GROUP BY 1


SELECT
  CONCAT(home_team_name, ' vs ', away_team_name) as game,
  latest(home_save_pct) * 100 as "Home Save %",
  latest(away_save_pct) * 100 as "Away Save %"
FROM nhl_games_analytics
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '60' MINUTE
GROUP BY 1


Select
    CONCAT(home_team_name, ' vs ', away_team_name) as game,
    latest("home_goals") as "Home Goals",
    latest("home_assists") as "Home Assists",
    latest("home_points") as "Home Points",
    latest("away_goals") as "Away Goals",
    latest("away_assists") as "Away Assists",
    latest("away_points") as "Away Points"
from nhl_games_analytics
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '60' MINUTE
GROUP BY 1