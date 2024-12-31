{{ config(
    materialized='incremental',
    unique_key=['game_id', 'bookmaker_key'],
    schema='core',
    incremental_strategy='merge',
    cluster_by=['partition_year', 'partition_month', 'partition_day']
) }}

with odds_data as (
    select
        game_id,
        bookmaker_key,
        date_trunc('day', ingestion_timestamp)::date as date_id,
        opening_home_price,
        opening_away_price,
        closing_home_price,
        closing_away_price,
        home_price_movement,
        away_price_movement,
        partition_year,
        partition_month,
        partition_day,
        'NBA' as sport_type,
        ingestion_timestamp
    from {{ ref('int_nba_odds_movement') }}

    union all

    select
        game_id,
        bookmaker_key,
        date_trunc('day', ingestion_timestamp)::date as date_id,
        opening_home_price,
        opening_away_price,
        closing_home_price,
        closing_away_price,
        home_price_movement,
        away_price_movement,
        partition_year,
        partition_month,
        partition_day,
        'NFL' as sport_type,
        ingestion_timestamp
    from {{ ref('int_nfl_odds_movement') }}

    union all

    select
        game_id,
        bookmaker_key,
        date_trunc('day', ingestion_timestamp)::date as date_id,
        opening_home_price,
        opening_away_price,
        closing_home_price,
        closing_away_price,
        home_price_movement,
        away_price_movement,
        partition_year,
        partition_month,
        partition_day,
        'NHL' as sport_type,
        ingestion_timestamp
    from {{ ref('int_nhl_odds_movement') }}

    union all

    select
        game_id,
        bookmaker_key,
        date_trunc('day', ingestion_timestamp)::date as date_id,
        opening_home_price,
        opening_away_price,
        closing_home_price,
        closing_away_price,
        home_price_movement,
        away_price_movement,
        partition_year,
        partition_month,
        partition_day,
        'CFB' as sport_type,
        ingestion_timestamp
    from {{ ref('int_cfb_odds_movement') }}
)

select * from odds_data
{% if is_incremental() %}
where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
{% endif %}
