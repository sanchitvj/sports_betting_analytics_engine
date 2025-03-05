{% macro market_efficiency(sport) %}
{{ config(
    materialized='incremental',
    unique_key=['game_id', 'bookmaker_key', 'bookmaker_last_update'],
    schema='int_layer',
    incremental_strategy='merge',
    cluster_by=['partition_year', 'partition_month', 'partition_day'],
    alias='int_' ~ sport ~ '_market_efficiency'
) }}

with efficiency_metrics as (
    select
        game_id,
        bookmaker_key,
        bookmaker_last_update,
        case
            when home_price > 0 then round(100/(home_price + 100) * 100, 3)
            else round(abs(home_price)/(abs(home_price) + 100) * 100, 3)
        end as home_implied_prob,
        case
            when away_price > 0 then round(100/(away_price + 100) * 100, 3)
            else round(abs(away_price)/(abs(away_price) + 100) * 100, 3)
        end as away_implied_prob,
        home_implied_prob + away_implied_prob - 100 as market_vig,
        case
            when home_implied_prob > away_implied_prob then 'HOME'
            else 'AWAY'
        end as market_favorite,
        abs(home_implied_prob - away_implied_prob) as probability_delta,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('stg_' ~ sport ~ '_odds') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
)
select * from efficiency_metrics

{% endmacro %}