SELECT
  CONCAT(home_team_name, ' vs ', away_team_name) as game,
  latest(home_team_score) as "Home Score",
  latest(away_team_score) as "Away Score"
FROM nba_games_analytics
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '60' MINUTE
GROUP BY 1


SELECT
  CONCAT(home_team_name, ' vs ', away_team_name) as game,
  CAST(latest(home_fg_pct) AS FLOAT) as "Home field goal %",
  CAST(latest(home_ft_pct) AS FLOAT) as "Home free throw %",
  CAST(latest(home_three_pt_pct) AS FLOAT) as "Home 3 pointer %",
  CAST(latest(away_fg_pct) AS FLOAT) as "Away field goal %",
  CAST(latest(away_ft_pct) AS FLOAT) as "Away free throw %",
  CAST(latest(away_three_pt_pct) AS FLOAT) as "Away 3 pointer %"
FROM nba_games_analytics
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '60' MINUTE
group by 1


SELECT
    CONCAT(home_team_name, ' vs ', away_team_name) as game,
    CAST(latest(home_assists) AS FLOAT) as "Home Assists",
    CAST(latest(home_rebounds) AS FLOAT) as "Home Rebounds",
    CAST(latest(away_assists) AS FLOAT) as "Away Assists",
    CAST(latest(away_rebounds) AS FLOAT) as "Away Rebounds"
FROM nba_games_analytics
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '60' MINUTE
group by 1