{{ config(
    materialized='incremental',
    unique_key=['game_id', 'bookmaker_key', 'sport_type'],
    schema='mart_analytics',
    incremental_strategy='merge',
    cluster_by=['partition_year', 'partition_month', 'partition_day']
) }}

with odds_analysis as (
    select
        game_id,
        bookmaker_key,
        'NBA' as sport_type,
        opening_home_price,
        opening_away_price,
        closing_home_price,
        closing_away_price,
        home_price_movement,
        away_price_movement,
        -- Movement Analysis
        case
            when abs(home_price_movement) <= 5 then 'Minimal'
            when abs(home_price_movement) <= 15 then 'Moderate'
            else 'Significant'
        end as movement_significance,
        -- Line Direction
        case
            when home_price_movement > 0 then 'Line Up'
            when home_price_movement < 0 then 'Line Down'
            else 'No Movement'
        end as line_movement_direction,
        -- Steam Move Detection
        case
            when abs(home_price_movement) >= 10
            and abs(closing_home_price - opening_home_price) <= 300 then true
            else false
        end as is_steam_move,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_nba_odds_movement') }}

    union all

    select
        game_id,
        bookmaker_key,
        'NFL' as sport_type,
        opening_home_price,
        opening_away_price,
        closing_home_price,
        closing_away_price,
        home_price_movement,
        away_price_movement,
        case
            when abs(home_price_movement) <= 3 then 'Minimal'
            when abs(home_price_movement) <= 7 then 'Moderate'
            else 'Significant'
        end as movement_significance,
        case
            when home_price_movement > 0 then 'Line Up'
            when home_price_movement < 0 then 'Line Down'
            else 'No Movement'
        end as line_movement_direction,
        case
            when abs(home_price_movement) >= 7
            and abs(closing_home_price - opening_home_price) <= 300 then true
            else false
        end as is_steam_move,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_nfl_odds_movement') }}

    union all

    select
        game_id,
        bookmaker_key,
        'NHL' as sport_type,
        opening_home_price,
        opening_away_price,
        closing_home_price,
        closing_away_price,
        home_price_movement,
        away_price_movement,
        case
            when abs(home_price_movement) <= 10 then 'Minimal'
            when abs(home_price_movement) <= 20 then 'Moderate'
            else 'Significant'
        end as movement_significance,
        case
            when home_price_movement > 0 then 'Line Up'
            when home_price_movement < 0 then 'Line Down'
            else 'No Movement'
        end as line_movement_direction,
        case
            when abs(home_price_movement) >= 15
            and abs(closing_home_price - opening_home_price) <= 300 then true
            else false
        end as is_steam_move,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_nhl_odds_movement') }}

    union all

    select
        game_id,
        bookmaker_key,
        'CFB' as sport_type,
        opening_home_price,
        opening_away_price,
        closing_home_price,
        closing_away_price,
        home_price_movement,
        away_price_movement,
        case
            when abs(home_price_movement) <= 3 then 'Minimal'
            when abs(home_price_movement) <= 7 then 'Moderate'
            else 'Significant'
        end as movement_significance,
        case
            when home_price_movement > 0 then 'Line Up'
            when home_price_movement < 0 then 'Line Down'
            else 'No Movement'
        end as line_movement_direction,
        case
            when abs(home_price_movement) >= 7
            and abs(closing_home_price - opening_home_price) <= 300 then true
            else false
        end as is_steam_move,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_cfb_odds_movement') }}
)

select * from odds_analysis
{% if is_incremental() %}
where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
{% endif %}
