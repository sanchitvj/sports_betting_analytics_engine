from datetime import datetime


class ProcessingConfig:
    # Game duration in minutes
    OWNER = "PENGUIN_DB"

    TAGS = {
        "nba_games": ["nba_games", "batch", "process"],
        "nba_odds": ["nba_odds", "batch", "process"],
        "nfl_games": ["nfl_games", "batch", "process"],
        "nfl_odds": ["nfl_odds", "batch", "process"],
        "nhl_games": ["nhl_games", "batch", "process"],
        "nhl_odds": ["nhl_odds", "batch", "process"],
        "cfb_games": ["cfb_games", "batch", "process"],
        "cfb_odds": ["cfb_odds", "batch", "process"],
        "nba_source_dqc": ["nba", "source", "data_quality_check"],
        "nhl_source_dqc": ["nhl", "source", "data_quality_check"],
        "nfl_source_dqc": ["nfl", "source", "data_quality_check"],
        "cfb_source_dqc": ["cfb", "source", "data_quality_check"],
    }

    S3_PATHS = {
        "raw_bucket": "raw-sp-data-aeb",
        "processing_bucket": "cur-sp-data-aeb",
        "games_prefix": "historical/games",
        "odds_prefix": "historical/odds",
        # "misc_bucket:" ""
    }

    GLUE_DB = {
        "db_name": "aeb_glue_db",
        "nba_games_table": "nba_games",
        "nba_odds_table": "nba_odds",
        "nfl_games_table": "nfl_games",
        "nfl_odds_table": "nfl_odds",
        "nhl_games_table": "nhl_games",
        "nhl_odds_table": "nhl_odds",
        "cfb_games_table": "cfb_games",
        "cfb_odds_table": "cfb_odds",
    }

    # League IDs
    LEAGUE_IDS = {"nfl": 1, "ncaa": 2, "nba": "standard", "nhl": 57}

    SPORT_CONFIGS = {
        "nba": {
            "start_date": datetime(2024, 10, 22),  # NBA season start
            "end_date": datetime(2024, 10, 26),
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

    SCRIPT_PATHS = {
        "base_path": "/home/ubuntu/sports_betting_analytics_engine/src/betflow/historical/batch_processing",
        "nba_games": "/home/ubuntu/sports_betting_analytics_engine/src/betflow/historical/batch_processing/nba_glue_job.py",
        "nhl_games": "/home/ubuntu/sports_betting_analytics_engine/src/betflow/historical/batch_processing/nhl_glue_job.py",
        "nfl_games": "/home/ubuntu/sports_betting_analytics_engine/src/betflow/historical/batch_processing/nfl_glue_job.py",
        "cfb_games": "/home/ubuntu/sports_betting_analytics_engine/src/betflow/historical/batch_processing/cfb_glue_job.py",
        "odds_processing": "/home/ubuntu/sports_betting_analytics_engine/src/betflow/historical/batch_processing/odds_glue_job.py",
    }
