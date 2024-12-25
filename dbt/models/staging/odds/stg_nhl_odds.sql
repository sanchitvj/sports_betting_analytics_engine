{{ config(
    materialized='incremental',
    unique_key=['game_id', 'bookmaker_key', 'bookmaker_last_update'],
    incremental_strategy='merge',
    cluster_by=['partition_year', 'partition_month', 'partition_day'],
    schema='silver_layer'
) }}

with source as (
    select VALUE from {{ source('raw_layer', 'nhl_odds') }}
    {% if is_incremental() %}
    where VALUE:ingestion_timestamp::timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
),
staged as (
    select
        VALUE:game_id::string as game_id,
        VALUE:sport_key::string as sport_key,
        VALUE:sport_title::string as sport_title,
        VALUE:commence_time::timestamp as commence_time,
        VALUE:home_team::string as home_team,
        VALUE:away_team::string as away_team,
        VALUE:bookmaker_key::string as bookmaker_key,
        VALUE:bookmaker_title::string as bookmaker_title,
        VALUE:bookmaker_last_update::timestamp as bookmaker_last_update,
        VALUE:market_key::string as market_key,
        VALUE:market_last_update::timestamp as market_last_update,
        VALUE:home_price::number as home_price,
        VALUE:away_price::number as away_price,
        VALUE:partition_year::integer as partition_year,
        VALUE:partition_month::integer as partition_month,
        VALUE:partition_day::integer as partition_day,
        VALUE:ingestion_timestamp::timestamp as ingestion_timestamp
    from source
        qualify row_number() over (
        partition by
            game_id,
            bookmaker_key,
            bookmaker_last_update
        order by ingestion_timestamp desc
    ) = 1
)
select * from staged