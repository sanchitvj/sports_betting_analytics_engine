{{ config(
    materialized='incremental',
    unique_key=['game_id', 'player_name', 'leader_type'],
    schema='mart_analytics',
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

        -- Achievement Tracking
        case
            when leader_type = 'GOALS' and stat_value >= 3 then 'Hat Trick'
            when leader_type = 'GOALS' and stat_value = 2 then 'Brace'
            when leader_type = 'POINTS' and stat_value >= 4 then 'Four Point Night'
            else 'Regular Performance'
        end as performance_milestone,

        -- Games as Leader Counter
        count(*) over (
            partition by player_name, leader_type
            order by game_id
            rows between unbounded preceding and current row
        ) as games_as_leader,

        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_nhl_leader_stats') }}
),

aggregated_stats as (
    select
        *,
        -- Achievement Counts
        sum(case when performance_milestone = 'Hat Trick' then 1 else 0 end) over (
            partition by player_name
            order by game_id
            rows between unbounded preceding and current row
        ) as total_hat_tricks,

        -- Performance Ranking
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
