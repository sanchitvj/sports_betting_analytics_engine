from datetime import datetime


class HistoricalConfig:
    # Game duration in minutes
    GAME_DURATIONS = {
        "nfl": 240,  # 4 hours including breaks
        "cfb": 240,  # 4 hours including breaks
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

    SPORT_CONFIGS = {
        "nba": {
            "endpoint": "basketball/nba/scoreboard",
            "start_date": datetime(
                2024, 10, 22
            ),  # NBA season start original:(2024, 10, 22) but data already present
            "end_date": datetime(2024, 12, 1),
        },
        "nfl": {
            "endpoint": "football/nfl/scoreboard",
            "start_date": datetime(2024, 9, 7),  # NFL season start
            "end_date": datetime(2024, 9, 8),
        },
        "nhl": {
            "endpoint": "hockey/nhl/scoreboard",
            "start_date": datetime(2024, 10, 10),  # NHL season start
            "end_date": datetime(2024, 10, 11),
        },
        "cfb": {
            "endpoint": "football/college-football/scoreboard",
            "start_date": datetime(2024, 8, 26),  # CFB season start
            "end_date": datetime(2024, 8, 27),
        },
    }
