ESPN_CONFIG = {
    "API_KEY": "your_espn_api_key",
    "BASE_URL": "http://site.api.espn.com/apis/site/v2/sports",
    "SPORTS": ["basketball/nba", "football/nfl", "baseball/mlb"],
}

KAFKA_CONFIG = {
    "BOOTSTRAP_SERVERS": "localhost:9092",
    "GAME_EVENTS_TOPIC": "game_events",
    "ANALYTICS_TOPIC": "game_analytics",
}

SPARK_CONFIG = {
    "APP_NAME": "game_pipeline_test",
    "MASTER": "local[2]",
    "CHECKPOINT_LOCATION": "/tmp/checkpoint",
}
