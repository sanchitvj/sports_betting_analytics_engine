{{ config(
    materialized='incremental',
    unique_key=['game_id', 'sport_type', 'bookmaker_last_update'],
    schema='mart_analytics',
    incremental_strategy='merge',
    cluster_by=['partition_year', 'partition_month', 'partition_day']
) }}

with bookmaker_metrics as (
    select
        game_id,
        'NBA' as sport_type,
        last_update as bookmaker_last_update,
        -- Market Competition
        number_of_bookmakers,
        array_size(participating_bookmakers) as active_bookmakers,
        -- Price Analysis
        avg_home_price,
        avg_away_price,
        home_price_spread,
        away_price_spread,
        -- Market Efficiency
        case
            when home_price_spread <= 5 then 'Tight'
            when home_price_spread <= 15 then 'Moderate'
            else 'Wide'
        end as market_spread_category,
        -- Competition Metrics
        case
            when number_of_bookmakers >= 8 then 'Highly Competitive'
            when number_of_bookmakers >= 5 then 'Competitive'
            else 'Limited Competition'
        end as market_competition_level,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_nba_bookmakers') }}

    union all

    select
        game_id,
        'NFL' as sport_type,
        last_update as bookmaker_last_update,
        number_of_bookmakers,
        array_size(participating_bookmakers) as active_bookmakers,
        avg_home_price,
        avg_away_price,
        home_price_spread,
        away_price_spread,
        case
            when home_price_spread <= 3 then 'Tight'
            when home_price_spread <= 10 then 'Moderate'
            else 'Wide'
        end as market_spread_category,
        case
            when number_of_bookmakers >= 8 then 'Highly Competitive'
            when number_of_bookmakers >= 5 then 'Competitive'
            else 'Limited Competition'
        end as market_competition_level,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_nfl_bookmakers') }}

    union all

    select
        game_id,
        'NHL' as sport_type,
        last_update as bookmaker_last_update,
        number_of_bookmakers,
        array_size(participating_bookmakers) as active_bookmakers,
        avg_home_price,
        avg_away_price,
        home_price_spread,
        away_price_spread,
        case
            when home_price_spread <= 10 then 'Tight'
            when home_price_spread <= 20 then 'Moderate'
            else 'Wide'
        end as market_spread_category,
        case
            when number_of_bookmakers >= 8 then 'Highly Competitive'
            when number_of_bookmakers >= 5 then 'Competitive'
            else 'Limited Competition'
        end as market_competition_level,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_nhl_bookmakers') }}

    union all

    select
        game_id,
        'CFB' as sport_type,
        last_update as bookmaker_last_update,
        number_of_bookmakers,
        array_size(participating_bookmakers) as active_bookmakers,
        avg_home_price,
        avg_away_price,
        home_price_spread,
        away_price_spread,
        case
            when home_price_spread <= 3 then 'Tight'
            when home_price_spread <= 10 then 'Moderate'
            else 'Wide'
        end as market_spread_category,
        case
            when number_of_bookmakers >= 8 then 'Highly Competitive'
            when number_of_bookmakers >= 5 then 'Competitive'
            else 'Limited Competition'
        end as market_competition_level,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_cfb_bookmakers') }}
)

select * from bookmaker_metrics
{% if is_incremental() %}
where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
{% endif %}
