{{ config(
    materialized='incremental',
    unique_key=['game_id', 'bookmaker_key', 'partition_year', 'partition_month', 'partition_day'],
    schema='mart_analytics',
    incremental_strategy='merge',
    cluster_by=['partition_year', 'partition_month', 'partition_day']
) }}

with market_analysis as (
    select
        game_id,
        bookmaker_key,
        
        'NBA' as sport_type,
        -- Market Efficiency Metrics
        avg(market_vig) as avg_vig,
        min(market_vig) as min_vig,
        max(market_vig) as max_vig,
        -- Market Classification
        case
            when avg(market_vig) <= 1.05 then 'Highly Efficient'
            when avg(market_vig) <= 1.10 then 'Efficient'
            when avg(market_vig) <= 1.15 then 'Moderately Efficient'
            else 'Inefficient'
        end as market_efficiency_rating,

        -- Probability Analysis
        avg(probability_delta) as avg_probability_spread,
        max(probability_delta) as max_probability_spread,

        -- Market Consensus
        mode(market_favorite) as consensus_favorite,
        count(distinct market_favorite) as favorite_changes,

        -- Time Analysis
        count(distinct bookmaker_last_update) as price_updates,
        partition_year,
        partition_month,
        partition_day,
        max(ingestion_timestamp) as last_updated
    from {{ ref('int_nba_market_efficiency') }}
    group by game_id, bookmaker_key, partition_year, partition_month, partition_day

    union all

    select
        game_id,
        bookmaker_key,
        
        'NFL' as sport_type,
        avg(market_vig) as avg_vig,
        min(market_vig) as min_vig,
        max(market_vig) as max_vig,
        case
            when avg(market_vig) <= 1.05 then 'Highly Efficient'
            when avg(market_vig) <= 1.10 then 'Efficient'
            when avg(market_vig) <= 1.15 then 'Moderately Efficient'
            else 'Inefficient'
        end as market_efficiency_rating,
        avg(probability_delta) as avg_probability_spread,
        max(probability_delta) as max_probability_spread,
        mode(market_favorite) as consensus_favorite,
        count(distinct market_favorite) as favorite_changes,
        count(distinct bookmaker_last_update) as price_updates,
        partition_year,
        partition_month,
        partition_day,
        max(ingestion_timestamp) as last_updated
    from {{ ref('int_nfl_market_efficiency') }}
    group by game_id, bookmaker_key, partition_year, partition_month, partition_day

    union all

    select
        game_id,
        bookmaker_key,
        
        'NHL' as sport_type,
        avg(market_vig) as avg_vig,
        min(market_vig) as min_vig,
        max(market_vig) as max_vig,
        case
            when avg(market_vig) <= 1.05 then 'Highly Efficient'
            when avg(market_vig) <= 1.10 then 'Efficient'
            when avg(market_vig) <= 1.15 then 'Moderately Efficient'
            else 'Inefficient'
        end as market_efficiency_rating,
        avg(probability_delta) as avg_probability_spread,
        max(probability_delta) as max_probability_spread,
        mode(market_favorite) as consensus_favorite,
        count(distinct market_favorite) as favorite_changes,
        count(distinct bookmaker_last_update) as price_updates,
        partition_year,
        partition_month,
        partition_day,
        max(ingestion_timestamp) as last_updated
    from {{ ref('int_nhl_market_efficiency') }}
    group by game_id, bookmaker_key, partition_year, partition_month, partition_day

    union all

    select
        game_id,
        bookmaker_key,
        
        'CFB' as sport_type,
        avg(market_vig) as avg_vig,
        min(market_vig) as min_vig,
        max(market_vig) as max_vig,
        case
            when avg(market_vig) <= 1.05 then 'Highly Efficient'
            when avg(market_vig) <= 1.10 then 'Efficient'
            when avg(market_vig) <= 1.15 then 'Moderately Efficient'
            else 'Inefficient'
        end as market_efficiency_rating,
        avg(probability_delta) as avg_probability_spread,
        max(probability_delta) as max_probability_spread,
        mode(market_favorite) as consensus_favorite,
        count(distinct market_favorite) as favorite_changes,
        count(distinct bookmaker_last_update) as price_updates,
        partition_year,
        partition_month,
        partition_day,
        max(ingestion_timestamp) as last_updated
    from {{ ref('int_cfb_market_efficiency') }}
    group by game_id, bookmaker_key, partition_year, partition_month, partition_day
)

select * from market_analysis
{% if is_incremental() %}
where last_updated > (select max(last_updated) from {{ this }})
{% endif %}
