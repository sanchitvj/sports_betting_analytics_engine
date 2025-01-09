{{ config(
    materialized='incremental',
    unique_key=['game_id', 'player_name', 'leader_type', 'sport_type'],
    schema='mart_analytics',
    incremental_strategy='merge',
    cluster_by=['partition_year', 'partition_month', 'partition_day']
) }}

with leader_metrics as (
    select
        game_id,
        player_name,
        leader_type,
        'NFL' as sport_type,
        team_id,
        stat_value,
        -- Performance Metrics
        avg(stat_value) over (
            partition by player_name, leader_type
            order by game_id
            rows between unbounded preceding and current row
        ) as avg_stat_value,

        -- Season Records
        max(stat_value) over (
            partition by player_name, leader_type, partition_year
        ) as season_high,

        -- Recent Form (Last 5 Games)
        avg(stat_value) over (
            partition by player_name, leader_type
            order by game_id
            rows between 4 preceding and current row
        ) as last_5_games_avg,

        -- Achievement Tracking
        case
            when leader_type = 'PASSING' and stat_value >= 300 then 1
            when leader_type = 'RUSHING' and stat_value >= 100 then 1
            when leader_type = 'RECEIVING' and stat_value >= 100 then 1
            else 0
        end as milestone_achievement,

        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_nfl_leader_stats') }}

    union all

    select
        game_id,
        player_name,
        leader_type,
        'CFB' as sport_type,
        team_id,
        stat_value,
        avg(stat_value) over (
            partition by player_name, leader_type
            order by game_id
            rows between unbounded preceding and current row
        ) as avg_stat_value,
        max(stat_value) over (
            partition by player_name, leader_type, partition_year
        ) as season_high,
        avg(stat_value) over (
            partition by player_name, leader_type
            order by game_id
            rows between 4 preceding and current row
        ) as last_5_games_avg,
        case
            when leader_type = 'PASSING' and stat_value >= 300 then 1
            when leader_type = 'RUSHING' and stat_value >= 100 then 1
            when leader_type = 'RECEIVING' and stat_value >= 100 then 1
            else 0
        end as milestone_achievement,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_cfb_leader_stats') }}
),

aggregated_stats as (
    select
        *,
        -- Cumulative Achievements
        sum(milestone_achievement) over (
            partition by player_name, leader_type, partition_year
            order by game_id
        ) as season_milestones,

        -- Performance Ranking
        percent_rank() over (
            partition by leader_type, sport_type, partition_year
            order by stat_value
        ) as performance_percentile
    from leader_metrics
)

select * from aggregated_stats
{% if is_incremental() %}
where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
{% endif %}
