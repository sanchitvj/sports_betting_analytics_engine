-- models/marts/analytics/mart_nba_leader_stats.sql
{{ config(
    materialized='incremental',
    unique_key=['game_id', 'player_name', 'leader_type'],
    schema='analytics',
    incremental_strategy='merge',
    cluster_by=['partition_year', 'partition_month', 'partition_day']
) }}

with leader_metrics as (
    select
        game_id,
        player_name,
        team_id,
        leader_type,
        stat_value,
        -- Running Stats
        avg(stat_value) over (
            partition by player_name, leader_type
            order by game_id
            rows between unbounded preceding and current row
        ) as avg_stat_value,

        max(stat_value) over (
            partition by player_name, leader_type
            order by game_id
            rows between unbounded preceding and current row
        ) as season_high,

        -- Last 5 Games Average
        avg(stat_value) over (
            partition by player_name, leader_type
            order by game_id
            rows between 4 preceding and current row
        ) as last_5_games_avg,

        -- Consistency Metrics
        count(*) over (
            partition by player_name, leader_type
            order by game_id
            rows between unbounded preceding and current row
        ) as games_as_leader,

        -- Performance Thresholds
        case
            when leader_type = 'POINTS' and stat_value >= 30 then 1
            when leader_type = 'REBOUNDS' and stat_value >= 15 then 1
            when leader_type = 'ASSISTS' and stat_value >= 10 then 1
            else 0
        end as exceptional_performance,

        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_nba_leader_stats') }}
),

aggregated_stats as (
    select
        *,
        -- Achievement Tracking
        sum(exceptional_performance) over (
            partition by player_name, leader_type
            order by game_id
            rows between unbounded preceding and current row
        ) as total_exceptional_games,

        -- Percentile Ranking
        percent_rank() over (
            partition by leader_type
            order by stat_value
        ) as performance_percentile
    from leader_metrics
)

select * from aggregated_stats
{% if is_incremental() %}
where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
{% endif %}
