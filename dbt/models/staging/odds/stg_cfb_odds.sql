{{ config(
    materialized='view',
    schema='silver_layer'
) }}

with source as (
    select DATA from {{ source('raw_data', 'cfb_odds') }}
),
staged as (
    select
        DATA:game_id::string as game_id,
        DATA:sport_key::string as sport_key,
        DATA:sport_title::string as sport_title,
        DATA:commence_time::timestamp as commence_time,
        DATA:home_team::string as home_team,
        DATA:away_team::string as away_team,
        DATA:bookmaker_key::string as bookmaker_key,
        DATA:bookmaker_title::string as bookmaker_title,
        DATA:bookmaker_last_update::timestamp as bookmaker_last_update,
        DATA:market_key::string as market_key,
        DATA:market_last_update::timestamp as market_last_update,
        DATA:home_price::number as home_price,
        DATA:away_price::number as away_price,
        DATA:partition_year::integer as partition_year,
        DATA:partition_month::integer as partition_month,
        DATA:partition_day::integer as partition_day,
        DATA:ingestion_timestamp::timestamp as ingestion_timestamp
    from source
)
select * from staged