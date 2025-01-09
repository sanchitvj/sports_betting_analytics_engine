{{ config(
    materialized='incremental',
    unique_key='game_id',
    schema='mart_core',
    incremental_strategy='merge',
    cluster_by=['partition_year', 'partition_month', 'partition_day']
) }}

with game_facts as (
    select
        game_id,
        start_time,
        date_trunc('day', start_time)::date as date_id,
        'NBA' as sport_type,
        home_name,
        away_name,
        home_score,
        away_score,
        score_difference,
        winner,
        total_periods,
        number_of_overtimes,
        home_ot_scores,
        away_ot_scores,
        venue_name,
        venue_city,
        venue_state,
        True as is_indoor,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_nba_game_stats') }}

    union all

    select
        game_id,
        start_time,
        date_trunc('day', start_time)::date as date_id,
        'NFL' as sport_type,
        home_name,
        away_name,
        home_score,
        away_score,
        score_difference,
        winner,
        total_periods,
        number_of_overtimes,
        home_ot_scores,
        away_ot_scores,
        venue_name,
        venue_city,
        venue_state,
        is_indoor,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_nfl_game_stats') }}

    union all

    select
        game_id,
        start_time,
        date_trunc('day', start_time)::date as date_id,
        'NHL' as sport_type,
        home_name,
        away_name,
        home_score,
        away_score,
        score_difference,
        winner,
        total_periods,
        number_of_overtimes,
        home_ot_scores,
        away_ot_scores,
        venue_name,
        venue_city,
        venue_state,
        is_indoor,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_nhl_game_stats') }}

    union all

    select
        game_id,
        start_time,
        date_trunc('day', start_time)::date as date_id,
        'CFB' as sport_type,
        home_name,
        away_name,
        home_score,
        away_score,
        score_difference,
        winner,
        total_periods,
        number_of_overtimes,
        home_ot_scores,
        away_ot_scores,
        venue_name,
        venue_city,
        venue_state,
        is_indoor,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_cfb_game_stats') }}
)

select * from game_facts
{% if is_incremental() %}
where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
{% endif %}
