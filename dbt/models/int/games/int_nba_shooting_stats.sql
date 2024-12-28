{{ config(
    materialized='incremental',
    unique_key=['game_id', 'team_type'],
    schema='int_layer',
    incremental_strategy='merge',
    cluster_by=['partition_year', 'partition_month', 'partition_day']
) }}

with shooting_stats as (
    select
        game_id,
        'HOME' as team_type,
        home_name as team_name,
        home_score as points_scored,
        -- Shooting Splits
        home_fg_pct as field_goal_pct,
        home_3p_pct as three_point_pct,
        home_ft_pct as free_throw_pct,
        -- Estimated Points Distribution
        round(home_score * (home_3p_pct/100) * 0.3) as points_from_threes,
        round(home_score * (home_ft_pct/100) * 0.2) as points_from_ft,
        -- Efficiency Metrics
        home_assists::float/home_rebounds as assist_to_rebound_ratio,
        home_score::float/
            (nullif(home_fg_pct::float/100 *
            (2 + home_3p_pct::float/100), 0)) as shot_attempts_estimate,
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
        away_name as team_name,
        away_score as points_scored,
        -- Similar metrics for away team
        away_fg_pct as field_goal_pct,
        away_3p_pct as three_point_pct,
        away_ft_pct as free_throw_pct,
        round(away_score * (away_3p_pct/100) * 0.3) as points_from_threes,
        round(away_score * (away_ft_pct/100) * 0.2) as points_from_ft,
        away_assists::float/away_rebounds as assist_to_rebound_ratio,
        away_score::float/
            (nullif(away_fg_pct::float/100 *
            (2 + away_3p_pct::float/100), 0)) as shot_attempts_estimate,
        partition_year, partition_month, partition_day
    from {{ ref('stg_nba_games') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
)
select * from shooting_stats
