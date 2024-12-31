{{ config(
    materialized='incremental',
    unique_key=['team_name', 'game_id'],
    schema='analytics',
    incremental_strategy='merge',
    cluster_by=['partition_year', 'partition_month', 'partition_day']
) }}

with team_stats as (
    select
        game_id,
        team_name,
        team_type,
        partition_year,
        partition_month,
        -- Scoring Stats
        avg(goals_scored) as avg_goals_per_game,
        avg(assists) as avg_assists_per_game,
        avg(saves) as avg_saves_per_game,
        avg(assists_per_goal) as avg_assists_per_goal,
        avg(actual_save_pct) as avg_save_percentage,

        -- Win/Loss Analysis
        sum(is_winner) as total_wins,
        count(*) as total_games,
        sum(is_winner)::float / count(*) * 100 as win_percentage,

        -- Overtime Performance
        sum(played_overtime) as total_overtime_games,
        sum(case when played_overtime = 1 and is_winner = 1 then 1 else 0 end) as overtime_wins,
        max(ingestion_timestamp) as last_updated
    from {{ ref('int_nhl_team_performance') }}
    group by 1, 2, 3, 4, 5
)

select * from team_stats
{% if is_incremental() %}
where last_updated > (select max(last_updated) from {{ this }})
{% endif %}
