{{ config(
    materialized='incremental',
    unique_key=['game_id', 'team_type'],
    schema='int_layer',
    incremental_strategy='merge',
    cluster_by=['partition_year', 'partition_month', 'partition_day']
) }}

with team_performance as (
    select
        game_id,
        'HOME' as team_type,
        home_id as team_id,
        home_name as team_name,
        home_abbreviation as team_abbreviation,
        home_score as points_scored,
        home_rebounds as rebounds,
        home_assists as assists,
        home_fg_pct as field_goal_pct,
        home_3p_pct as three_point_pct,
        home_ft_pct as free_throw_pct,
        -- Calculated Metrics
        home_assists::float/nullif(home_score, 0) as assists_per_point,
        home_rebounds::float/nullif(home_score + away_score, 0) as rebound_share,
        case when home_score > away_score then 1 else 0 end as is_winner,
        partition_year, partition_month, partition_day,
        ingestion_timestamp
    from {{ ref('stg_nba_games') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}

    union all

    select
        game_id,
        'AWAY' as team_type,
        away_id as team_id,
        away_name as team_name,
        away_abbreviation as team_abbreviation,
        away_score as points_scored,
        away_rebounds as rebounds,
        away_assists as assists,
        away_fg_pct as field_goal_pct,
        away_3p_pct as three_point_pct,
        away_ft_pct as free_throw_pct,
        away_assists::float/nullif(away_score, 0) as assists_per_point,
        away_rebounds::float/nullif(home_score + away_score, 0) as rebound_share,
        case when away_score > home_score then 1 else 0 end as is_winner,
        partition_year, partition_month, partition_day,
        ingestion_timestamp
    from {{ ref('stg_nba_games') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
)
select * from team_performance

