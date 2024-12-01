SELECT
  CONCAT(home_team_name, ' vs ', away_team_name) as game,
  latest(home_team_score) as "Home Score",
  latest(away_team_score) as "Away Score"
  -- status_state,
  -- current_period as quarter,
  -- time_remaining
FROM nfl_games_analytics
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '60' MINUTE
GROUP BY 1--, 2, 3--, 4, 5, 6