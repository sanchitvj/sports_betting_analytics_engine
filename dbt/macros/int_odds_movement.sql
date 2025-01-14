{% macro odds_movement(sport) %}
{{ config(
    materialized='incremental',
    unique_key=['game_id', 'bookmaker_key', 'bookmaker_last_update'],
    schema='int_layer',
    incremental_strategy='merge',
    cluster_by=['partition_year', 'partition_month', 'partition_day'],
    alias='int_' ~ sport ~ '_odds_movement'
) }}

with odds_timeline as (
    select
        game_id,
        bookmaker_key,
        bookmaker_last_update,
        -- First odds (opening)
        first_value(home_price) over (
            partition by game_id, bookmaker_key
            order by bookmaker_last_update
        ) as opening_home_price,
        first_value(away_price) over (
            partition by game_id, bookmaker_key
            order by bookmaker_last_update
        ) as opening_away_price,
        -- Latest odds (closing)
        last_value(home_price) over (
            partition by game_id, bookmaker_key
            order by bookmaker_last_update
        ) as closing_home_price,
        last_value(away_price) over (
            partition by game_id, bookmaker_key
            order by bookmaker_last_update
        ) as closing_away_price,
        -- Movement calculation
        closing_home_price - opening_home_price as home_price_movement,
        closing_away_price - opening_away_price as away_price_movement,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('stg_' ~ sport ~ '_odds') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
)
select * from odds_timeline

{% endmacro %}