from datetime import datetime
import os
import boto3
import aiohttp
import asyncio
from airflow.exceptions import AirflowException

from betflow.historical.hist_api_connectors import (
    ESPNBatchConnector,
    HistoricalOddsConnector,
)
from betflow.historical.config import HistoricalConfig
from betflow.api_connectors.raw_game_transformers import (
    api_raw_nba_data,
    api_raw_nfl_data,
    api_raw_nhl_data,
    api_raw_cfb_data,
)


def check_games_data(sport_key, **context):
    """Check if games data exists for the sport"""
    try:
        games_data = context["task_instance"].xcom_pull(
            task_ids=f"{sport_key}_pipeline.fetch_{sport_key}_games",
            # key=f"{sport_key}_games_data",
            key="return_value",
        )
        return True if games_data else False
    except Exception as e:
        print(f"No games data found for {sport_key}: {str(e)}")
        return False


def fetch_games_by_date(sport_key, **context):
    """Fetch games for a specific date and sport"""
    logical_date = context.get("data_interval_start")  # - timedelta(days=1)
    date_str = logical_date.strftime("%Y%m%d")

    try:
        espn_connector = ESPNBatchConnector()
        endpoint = HistoricalConfig.SPORT_CONFIGS[sport_key]["endpoint"]
        params = {"dates": date_str}

        raw_data = espn_connector.make_request(endpoint, params)

        # Check if events exist
        if not raw_data.get("events"):
            print(f"No games found for date {date_str}")
            return 0  # Return success but zero games

        processed_games = []
        for game in raw_data.get("events", []):
            if sport_key == "nba":
                game_data = api_raw_nba_data(game)
            elif sport_key == "nfl":
                game_data = api_raw_nfl_data(game)
            elif sport_key == "nhl":
                game_data = api_raw_nhl_data(game)
            elif sport_key == "cfb":
                game_data = api_raw_cfb_data(game)

            if game_data["status_state"] in ["post", "STATUS_FINAL"]:
                processed_games.append(game_data)

        if processed_games:
            context["task_instance"].xcom_push(
                key=f"{sport_key}_games_data", value=processed_games
            )
            return processed_games
        return []

    except Exception as e:
        print(f"Error fetching {sport_key} games for date {date_str}: {str(e)}")
        raise


def upload_to_s3_func(sport_key: str, kind: str, **context):
    """Upload processed games and odds data to S3"""
    date_str = context["ds"]
    date_frmt = datetime.strptime(date_str, "%Y-%m-%d")  # - timedelta(days=1)
    date_str = date_frmt.strftime("%Y-%m-%d")

    local_path = f"/tmp/historical/{kind}/{sport_key}/{date_str}/{kind}.json"
    s3_path = f"historical/{kind}/{sport_key}/{date_str}/{kind}.json"

    if not os.path.exists(local_path):
        print(
            f"No {kind} data file found for {sport_key} on {date_str}, skipping upload"
        )
        return 0

    s3_client = boto3.client("s3")
    try:
        s3_client.upload_file(
            local_path, HistoricalConfig.S3_PATHS["raw_bucket"], s3_path
        )
        return 1
    except Exception as e:
        print(f"Error uploading {sport_key} data to S3: {str(e)}")
        raise


def fetch_odds_by_date(sport_key, **context):
    """Fetch odds data for games on a specific date"""

    async def _fetch():
        logical_date = context.get("data_interval_start")  # - timedelta(days=1)
        date_str = logical_date.strftime("%Y-%m-%d")
        # Get completed games for the date from previous games DAG
        # games_data = context["task_instance"].xcom_pull(
        #     dag_id="sports_ingestion_dqc",
        #     task_ids=f"{sport_key}_pipeline.validate_{sport_key}_json",
        #     key="return_value",
        # )
        #
        # if not games_data:
        #     print(f"No completed games found for date {date_str}")
        #     return []

        try:
            odds_connector = HistoricalOddsConnector(api_key=os.getenv("ODDS_API_KEY"))

            async with aiohttp.ClientSession() as session:
                # Fetch all odds for the date in one request
                odds_data = await odds_connector.fetch_odds_by_date(
                    session, sport_key, date_str
                )

                if odds_data:
                    context["task_instance"].xcom_push(
                        key=f"{sport_key}_odds_data", value=odds_data
                    )
                    return odds_data
                return []

        except Exception as e:
            print(f"Error fetching odds: {str(e)}")
            raise AirflowException(f"Failed to fetch odds: {str(e)}")

    return asyncio.run(_fetch())
