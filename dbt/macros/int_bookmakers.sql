{% macro compare_bookmakers(sport) %}
{{ config(
    materialized='table',
    unique_key=['game_id', 'market_key', 'last_update'],
    schema='int_layer',
    cluster_by=['partition_year', 'partition_month', 'partition_day'],
    alias='int_' ~ sport ~ '_bookmakers'
) }}

with market_analysis as (
    select
        game_id,
        market_key,
        bookmaker_last_update as last_update,
        -- Market consensus
        avg(home_price) as avg_home_price,
        avg(away_price) as avg_away_price,
        -- Price ranges
        max(home_price) - min(home_price) as home_price_spread,
        max(away_price) - min(away_price) as away_price_spread,
        -- Best available prices
        max(home_price) as best_home_price,
        max(away_price) as best_away_price,
        -- Bookmaker offering best price
        array_agg(distinct bookmaker_key) as participating_bookmakers,
        count(distinct bookmaker_key) as number_of_bookmakers,
        partition_year,
        partition_month,
        partition_day,
        max(ingestion_timestamp) as ingestion_timestamp
    from {{ ref('stg_' ~ sport ~ '_odds') }}
--     {% if is_incremental() %}
--     where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
--     {% endif %}
    group by 1, 2, 3, partition_year, partition_month, partition_day
)
select * from market_analysis

{% endmacro %}