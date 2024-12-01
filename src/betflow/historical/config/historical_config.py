class HistoricalConfig:
    # Game duration in minutes
    GAME_DURATIONS = {
        "nfl": 240,  # 4 hours including breaks
        "ncaa": 240,  # 4 hours including breaks
        "nba": 180,  # 3 hours including breaks
        "nhl": 180,  # 3 hours including breaks
    }

    # Odds snapshot interval in minutes
    ODDS_INTERVAL = 30

    # Game status mappings
    FINISHED_STATUSES = [
        "FT",
        "AOT",
        "Finished",
        "After Overtime",
    ]  # Finished, After Overtime

    # S3 paths
    S3_PATHS = {
        "raw_bucket": "raw-sp-data-aeb",
        "games_prefix": "historical/games",
        "odds_prefix": "historical/odds",
    }

    # Batch processing configs
    BATCH_SIZE = 50

    # League IDs
    LEAGUE_IDS = {"nfl": 1, "ncaa": 2, "nba": "standard", "nhl": 57}
