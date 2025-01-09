{{ config(
    materialized='incremental',
    unique_key=['team_name', 'game_id'],
    schema='mart_analytics',
    incremental_strategy='merge',
    cluster_by=['partition_year', 'partition_month', 'partition_day']
) }}

with team_stats as (
    select
        team_name,
        game_id,
        team_type,
        partition_year,
        partition_month,
        -- Scoring Trends
        avg(points_scored) as avg_points_per_game,
        max(points_scored) as highest_points,
        min(points_scored) as lowest_points,
        stddev(points_scored) as points_volatility,

        -- Shooting Efficiency
        avg(field_goal_pct) as avg_fg_pct,
        avg(three_point_pct) as avg_3p_pct,
        avg(free_throw_pct) as avg_ft_pct,

        -- Performance Metrics
        avg(assists) as avg_assists,
        avg(rebounds) as avg_rebounds,
        avg(assists_per_point) as avg_assists_per_point,
        avg(rebound_share) as avg_rebound_share,

        -- Win/Loss Analysis
        sum(is_winner) as total_wins,
        count(*) as total_games,
        sum(is_winner)::float / count(*) * 100 as win_percentage,

        -- Home/Away Split
        sum(case when team_type = 'HOME' then is_winner else 0 end) as home_wins,
        sum(case when team_type = 'HOME' then 1 else 0 end) as home_games,
        sum(case when team_type = 'AWAY' then is_winner else 0 end) as away_wins,
        sum(case when team_type = 'AWAY' then 1 else 0 end) as away_games,

        -- Scoring Distribution
        percentile_cont(0.25) within group (order by points_scored) as points_25th_percentile,
        percentile_cont(0.75) within group (order by points_scored) as points_75th_percentile,

        -- Form Analysis
        avg(points_scored) over (
            partition by team_name
            order by partition_year, partition_month
            rows between 5 preceding and current row
        ) as last_6_games_avg_points,

        max(ingestion_timestamp) as last_updated
    from {{ ref('int_nba_team_performance') }}
    group by 1, 2, 3, 4, 5
)

select
    *,
    points_75th_percentile - points_25th_percentile as points_interquartile_range,
    home_wins::float / nullif(home_games, 0) * 100 as home_win_percentage,
    away_wins::float / nullif(away_games, 0) * 100 as away_win_percentage
from team_stats
{% if is_incremental() %}
where last_updated > (select max(last_updated) from {{ this }})
{% endif %}
