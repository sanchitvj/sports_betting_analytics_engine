SELECT
  CONCAT(home_team_name, ' vs ', away_team_name) as game,
  latest(home_team_score) as "Home Score",
  latest(away_team_score) as "Away Score"
FROM cfb_games_analytics
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '60' MINUTE
GROUP BY 1


SELECT
    CONCAT(home_team_name, '(', home_team_id, ')', ' vs ', away_team_name, '(', away_team_id, ')') as game,
    latest(home_team_record) as "Home Record",
    latest(away_team_record) as "Away Record",
    latest(passing_leader_name) as "Pass Name",
    latest(passing_leader_display_value) as "Pass Display Value",
    latest(passing_leader_team) as "Pass Team",
    latest(rushing_leader_name) as "Rush Name",
    latest(rushing_leader_display_value) as "Rush Display Value",
    latest(rushing_leader_team) as "Rush Team",
    latest(receiving_leader_name) as "Receive Name",
    latest(receiving_leader_display_value) as "Receive Display Value",
    latest(receiving_leader_team) as "Receive Team"
from cfb_games_analytics
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '60' MINUTE
GROUP BY 1