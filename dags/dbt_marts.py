# from airflow import DAG
# from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
# from datetime import timedelta, datetime
# from airflow.operators.empty import EmptyOperator
# from betflow.historical.config import ProcessingConfig
#
# profile_config = ProfileConfig(
#     profile_name="betflow",
#     target_name="dev",
#     profiles_yml_filepath="/home/ubuntu/.dbt/profiles.yml",
# )
#
# project_config = ProjectConfig(
#     dbt_project_path="/home/ubuntu/sports_betting_analytics_engine/dbt/"
# )
#
# default_args = {
#     "owner": ProcessingConfig.OWNER,
#     "depends_on_past": True,
#     "email_on_failure": False,
#     "retries": 0,
#     "retry_delay": timedelta(minutes=5),
#     "tags": ["dbt", "analytics", "mart", "gold"],
# }
#
# with DAG(
#     "sports_betting_marts_dag",
#     default_args=default_args,
#     description="dbt models for mart layer",
#     schedule_interval="@daily",
#     start_date=datetime(2025, 1, 14),  # due to parent DAG
#     catchup=True,
# ) as dag:
#     start = EmptyOperator(task_id="start")
#
#     models = DbtTaskGroup(
#         group_id="mart_models",
#         project_config=project_config,
#         profile_config=profile_config,
#         render_config=RenderConfig(
#             select=[
#                 "+dim_teams +dim_venues +dim_dates +dim_bookmakers "
#                 "+fct_games +fct_odds "
#                 "+fct_betting_value +fct_bookmaker_perf "
#                 "+fct_football_leader_stats +fct_game_scoring_pattern "
#                 "+fct_market_efficiency +fct_nba_leader_stats "
#                 "+fct_nba_team_trends +fct_nhl_team_trends "
#                 "+fct_nhl_leader_stats +fct_odds_movement "
#                 "+fct_overtime_analysis +fct_record_matchup "
#             ]
#         ),
#     )
#
#     end = EmptyOperator(task_id="end", trigger_rule="all_done")
#
#     start >> models >> end
