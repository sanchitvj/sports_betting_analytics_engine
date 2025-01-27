from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from datetime import timedelta, datetime
from airflow.operators.empty import EmptyOperator
from betflow.historical.config import ProcessingConfig
from airflow.utils.state import DagRunState
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


profile_config = ProfileConfig(
    profile_name="betflow",
    target_name="dev",
    profiles_yml_filepath="/home/ubuntu/.dbt/profiles.yml",
)

project_config = ProjectConfig(
    dbt_project_path="/home/ubuntu/sports_betting_analytics_engine/dbt/"
)

default_args = {
    "owner": ProcessingConfig.OWNER,
    "depends_on_past": True,
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "tags": ["dbt", "analytics", "mart", "gold"],
}

with DAG(
    "snowflake_dbt_staging_marts",
    default_args=default_args,
    description="Refresh snowflake tables and dbt models for mart layer",
    # schedule_interval="@daily",
    schedule_interval=None,  # triggered by parent DAG
    start_date=datetime(2024, 12, 16),  # due to parent DAG
    catchup=True,
) as dag:
    start = EmptyOperator(task_id="start")

    wait_for_sports_batch = ExternalTaskSensor(
        task_id="wait_for_sports_batch",
        external_dag_id="sports_batch_processing",
        external_task_id=None,  # Wait for entire DAG
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
        execution_date_fn=lambda dt: dt,
        mode="poke",
        timeout=7200,
        poke_interval=60,
        soft_fail=True,
        check_existence=True,
    )

    wait_for_odds_batch = ExternalTaskSensor(
        task_id="wait_for_odds_batch",
        external_dag_id="odds_batch_processing",
        external_task_id=None,
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
        execution_date_fn=lambda dt: dt,
        mode="poke",
        timeout=7200,
        poke_interval=60,
        soft_fail=True,
        check_existence=True,
    )

    refresh_external_tables = SnowflakeOperator(
        task_id="refresh_external_tables",
        snowflake_conn_id="snowflake_conn",
        sql="""
            ALTER EXTERNAL TABLE sports_db.raw_layer.nba_games REFRESH;
            ALTER EXTERNAL TABLE sports_db.raw_layer.nba_odds REFRESH;
            ALTER EXTERNAL TABLE sports_db.raw_layer.nfl_games REFRESH;
            ALTER EXTERNAL TABLE sports_db.raw_layer.nfl_odds REFRESH;
            ALTER EXTERNAL TABLE sports_db.raw_layer.nhl_games REFRESH;
            ALTER EXTERNAL TABLE sports_db.raw_layer.nhl_odds REFRESH;
            ALTER EXTERNAL TABLE sports_db.raw_layer.cfb_games REFRESH;
            ALTER EXTERNAL TABLE sports_db.raw_layer.cfb_odds REFRESH;
            """,
    )

    models = DbtTaskGroup(
        group_id="mart_models",
        project_config=project_config,
        profile_config=profile_config,
        render_config=RenderConfig(
            select=[
                "+dim_teams +dim_venues +dim_dates +dim_bookmakers "
                "+fct_games +fct_odds "
                "+fct_betting_value +fct_bookmaker_perf "
                "+fct_football_leader_stats +fct_game_scoring_pattern "
                "+fct_market_efficiency +fct_nba_leader_stats "
                "+fct_nba_team_trends +fct_nhl_team_trends "
                "+fct_nhl_leader_stats +fct_odds_movement "
                "+fct_overtime_analysis +fct_record_matchup "
            ]
        ),
    )

    end = EmptyOperator(task_id="end", trigger_rule="all_success")

    (
        start
        >> [wait_for_sports_batch, wait_for_odds_batch]
        >> refresh_external_tables
        >> models
        >> end
    )
