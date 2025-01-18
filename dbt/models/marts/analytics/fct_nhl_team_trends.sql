{{ config(
    materialized='incremental',
    unique_key=['team_name', 'partition_year', 'partition_month'],
    schema='mart_analytics',
    incremental_strategy='merge',
    cluster_by=['partition_year', 'partition_month']
) }}

with team_stats as (
    select
        team_name,
        array_agg(array_construct(
            game_id,
            team_type,
            goals_scored
        )) within group (order by partition_year, partition_month, partition_day) as game_sequence_id_type_points,

        -- Scoring Stats
        round(avg(goals_scored), 3) as avg_goals_per_month,
        max(goals_scored) as max_goals_scored,
        min(goals_scored) as min_goals_scored,
        round(avg(assists), 3) as avg_assists_per_month,
        round(avg(saves), 3) as avg_saves_per_month,
        round(avg(assists_per_goal), 3) as avg_assists_per_goal,
        round(avg(actual_save_pct) * 100, 3) as avg_save_percentage,

        -- Win/Loss Analysis
        sum(is_winner) as total_wins,
        count(*) as total_games,
        round(sum(is_winner)::float / count(*) * 100, 3) as win_percentage,

        sum(case when team_type = 'HOME' then is_winner else 0 end) as home_wins,
        sum(case when team_type = 'HOME' then 1 else 0 end) as home_games,
        sum(case when team_type = 'AWAY' then is_winner else 0 end) as away_wins,
        sum(case when team_type = 'AWAY' then 1 else 0 end) as away_games,

        sum(played_overtime) as total_overtime_games,
        sum(case when played_overtime = 1 and is_winner = 1 then 1 else 0 end) as overtime_wins,
        round(percentile_cont(0.25) within group (order by goals_scored), 3) as points_25th_percentile,
        round(percentile_cont(0.75) within group (order by goals_scored), 3) as points_75th_percentile,

        partition_year,
        partition_month,
        max(ingestion_timestamp) as ingestion_timestamp
    from {{ ref('int_nhl_team_performance') }}
    group by team_name, partition_year, partition_month
)

select *,
    round(points_75th_percentile - points_25th_percentile, 3) as points_interquartile_range,
    round(home_wins::float / nullif(home_games, 0) * 100, 3) as home_win_percentage,
    round(away_wins::float / nullif(away_games, 0) * 100, 3) as away_win_percentage
from team_stats
{% if is_incremental() %}
where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
{% endif %}
