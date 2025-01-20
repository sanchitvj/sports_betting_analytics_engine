{{ config(
    materialized='incremental',
    unique_key=['team_name', 'partition_year', 'partition_month'],
    schema='mart_analytics',
    cluster_by=['partition_year', 'partition_month'],
    incremental_strategy='merge',
) }}

with team_stats as (
    select
        team_name,
        array_agg(array_construct(
            game_id,
            team_type,
            points_scored
        )) within group (order by partition_year, partition_month, partition_day) as game_sequence_id_type_points,
        -- Scoring Trends
        round(avg(points_scored), 3) as avg_points_per_game,
        max(points_scored) as highest_points,
        min(points_scored) as lowest_points,
        round(stddev(points_scored), 3) as points_volatility,

        -- Shooting Efficiency
        round(avg(field_goal_pct), 3) as avg_fg_pct,
        round(avg(three_point_pct), 3) as avg_3p_pct,
        round(avg(free_throw_pct), 3) as avg_ft_pct,

        -- Performance Metrics
        round(avg(assists), 3) as avg_assists,
        round(avg(rebounds), 3) as avg_rebounds,
        round(avg(assists_per_point), 3) as avg_assists_per_point,
        round(avg(rebound_share), 3) as avg_rebound_share,

        -- Win/Loss Analysis
        sum(is_winner) as total_wins,
        count(*) as total_games,
        round(sum(is_winner)::float / count(*) * 100, 3) as win_percentage,

        -- Home/Away Split
        sum(case when team_type = 'HOME' then is_winner else 0 end) as home_wins,
        sum(case when team_type = 'HOME' then 1 else 0 end) as home_games,
        sum(case when team_type = 'AWAY' then is_winner else 0 end) as away_wins,
        sum(case when team_type = 'AWAY' then 1 else 0 end) as away_games,

        -- Scoring Distribution
        round(percentile_cont(0.25) within group (order by points_scored), 3) as points_25th_percentile,
        round(percentile_cont(0.75) within group (order by points_scored), 3) as points_75th_percentile,

        partition_year,
        partition_month,
        max(ingestion_timestamp) as ingestion_timestamp
    from {{ ref('int_nba_team_performance') }}
    group by team_name, partition_year, partition_month
),
final_stats as (
    select
        *,
--         round(avg(avg_points_per_game) over (
--             partition by team_name
--             order by partition_year, partition_month
--             rows between 5 preceding and current row
--         ), 3) as last_6_games_avg_points,
        round(points_75th_percentile - points_25th_percentile, 3) as points_interquartile_range,
        round(home_wins::float / nullif(home_games, 0) * 100, 3) as home_win_percentage,
        round(away_wins::float / nullif(away_games, 0) * 100, 3) as away_win_percentage
    from team_stats
)

select * from final_stats
{% if is_incremental() %}
where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
{% endif %}
