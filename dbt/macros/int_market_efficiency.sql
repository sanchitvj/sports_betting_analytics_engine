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
        -- Implied probabilities
        case
            when home_price > 0 then abs(100/(home_price + 100))
            else abs(abs(home_price)/(abs(home_price) + 100))
        end as home_implied_prob,
        case
            when away_price > 0 then abs(100/(away_price + 100))
            else abs(abs(away_price)/(abs(away_price) + 100))
        end as away_implied_prob,
        -- Market efficiency metrics
        case
            -- For negative odds (favorites)
            when home_price < 0 then abs(home_price)/(abs(home_price) + 100)
            -- For positive odds (underdogs)
            else 100/(home_price + 100)
        end +
        case
            when away_price < 0 then abs(away_price)/(abs(away_price) + 100)
            else 100/(away_price + 100)
        end as market_vig,
        case
            when home_price > away_price then 'AWAY'
            else 'HOME'
        end as market_favorite,
        abs(
            case
                when home_price > 0 then abs(100/(home_price + 100))
                else abs(abs(home_price)/(abs(home_price) + 100))
            end -
            case
                when away_price > 0 then abs(100/(away_price + 100))
                else abs(abs(away_price)/(abs(away_price) + 100))
            end
        ) as probability_delta,
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