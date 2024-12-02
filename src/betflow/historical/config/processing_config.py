from datetime import datetime


class ProcessingConfig:
    # Game duration in minutes
    OWNER = "PENGUIN_DB"

    TAGS = {
        "nba_games": ["nba_games", "batch", "process"],
        "nfl_games": ["nfl_games", "batch", "process"],
        "nhl_games": ["nhl_games", "batch", "process"],
        "cfb_games": ["cfb_games", "batch", "process"],
    }

    S3_PATHS = {
        "raw_bucket": "raw-sp-data-aeb",
        "processing_bucket": "cur-sp-data-aeb",
        "games_prefix": "historical/games",
        "odds_prefix": "historical/odds",
    }

    GLUE_DB = {
        "db_name": "aeb_glue_db",
        "nba_games_table": "nba_games",
        "nfl_games_table": "nfl_games",
        "nhl_games_table": "nhl_games",
        "cfb_games_table": "cfb_games",
    }

    # League IDs
    LEAGUE_IDS = {"nfl": 1, "ncaa": 2, "nba": "standard", "nhl": 57}

    SPORT_CONFIGS = {
        "nba": {
            "start_date": datetime(2024, 10, 22),  # NBA season start
            "end_date": datetime(2024, 12, 1),
        },
        "nfl": {
            "start_date": datetime(2024, 9, 7),  # NFL season start
            "end_date": datetime(2024, 12, 1),
        },
        "nhl": {
            "start_date": datetime(2024, 10, 10),  # NHL season start
            "end_date": datetime(2024, 12, 1),
        },
        "cfb": {
            "start_date": datetime(2024, 8, 26),  # CFB season start
            "end_date": datetime(2024, 12, 1),
        },
    }
