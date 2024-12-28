{% macro market_efficiency(sport) %}
{{ config(
    materialized='incremental',
    unique_key=['game_id', 'bookmaker_key', 'bookmaker_last_update'],
    schema='int_layer',
    incremental_strategy='merge',
    cluster_by=['partition_year', 'partition_month', 'partition_day'],
    alias='int_' ~ sport ~ 'market_efficiency'
) }}

with efficiency_metrics as (
    select
        game_id,
        bookmaker_key,
        bookmaker_last_update,
        -- Implied probabilities
        1/nullif(home_price, 0) as home_implied_prob,
        1/nullif(away_price, 0) as away_implied_prob,
        -- Market efficiency metrics
        (1/nullif(home_price, 0) + 1/nullif(away_price, 0)) as market_vig,
        case
            when home_price > away_price then 'AWAY'
            else 'HOME'
        end as market_favorite,
        abs(1/nullif(home_price, 0) - 1/nullif(away_price, 0)) as probability_delta,
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