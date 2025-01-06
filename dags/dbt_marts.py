from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from datetime import timedelta, datetime
from airflow.operators.empty import EmptyOperator
from betflow.historical.config import ProcessingConfig

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profiles_yml_filepath="/home/ubuntu/.dbt/profiles.yml",
)

project_config = ProjectConfig(
    dbt_project_path="/home/ubuntu/sports_betting_analytics_engine/dbt_project.yml"
)

default_args = {
    "owner": ProcessingConfig.OWNER,
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "tags": ["dbt", "analytics", "mart", "gold"],
}

with DAG(
    "sports_betting_marts_dag",
    default_args=default_args,
    description="dbt models for mart layer",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),  # due to parent DAG
    catchup=True,
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # Core Models with Dependencies
    core_models = DbtTaskGroup(
        group_id="core_models",
        project_config=project_config,
        profile_config=profile_config,
        # Build core models and their dependencies
        commands=[
            "dbt build -s +dim_teams +dim_venues +dim_dates +dim_bookmakers +fct_games +fct_odds"
        ],
    )

    # Analytics Models with Dependencies
    analytics_models = DbtTaskGroup(
        group_id="analytics_models",
        project_config=project_config,
        profile_config=profile_config,
        # Build analytics models and their dependencies
        commands=[
            "dbt build -s +mart_game_scoring_patterns"
            " +mart_nba_team_trends +mart_nhl_team_trends +mart_nba_leader_stats"
            " +mart_football_leader_stats +mart_nhl_leader_stats +mart_odds_movement"
            " +mart_market_efficiency +mart_bookmaker_analysis +mart_betting_value"
            " +mart_overtime_analysis +mart_record_matchups"
        ],
    )

    # Define dependencies
    core_models >> analytics_models
