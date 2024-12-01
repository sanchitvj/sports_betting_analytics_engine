SELECT
  location as "Location",
  state_code as "State",
  game_name as "Game",
  ROUND(latest(avg_feels_like), 1) as "Feels Like (°C)",
  ROUND(latest(comfort_index), 1) as "Comfort Index",
  latest(current_condition) as "Condition",
  latest(weather_severity) as "Severity",
  ROUND(latest(avg_wind_speed), 1) as "Wind Speed m/s",
  latest(last_wind_direction) as "Direction",
  ROUND(latest(wind_impact_score), 2) as "Impact Score",
  Round(latest(avg_visibility), 1) as "Visibility in km",
  latest(current_condition) as "Current Condition",
  latest(current_description) as "Current Description"
FROM cfb_weather_analytics
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR
GROUP BY 1, 2, 3