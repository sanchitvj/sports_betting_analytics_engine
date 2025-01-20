{{ config(
    materialized='table',
    unique_key=['game_id', 'bookmaker_key'],
    schema='mart_core'
) }}

with market_metrics as (
    select distinct
        game_id,
        bookmaker_key,
        market_vig,
        market_favorite,
        probability_delta,
        'NBA' as sport_type,
    from {{ ref('int_nba_market_efficiency') }}

    union distinct

    select distinct
        game_id,
        bookmaker_key,
        market_vig,
        market_favorite,
        probability_delta,
        'NFL' as sport_type,
    from {{ ref('int_nfl_market_efficiency') }}

    union distinct

    select distinct
        game_id,
        bookmaker_key,
        market_vig,
        market_favorite,
        probability_delta,
        'NHL' as sport_type,
    from {{ ref('int_nhl_market_efficiency') }}

    union distinct

    select distinct
        game_id,
        bookmaker_key,
        market_vig,
        market_favorite,
        probability_delta,
        'CFB' as sport_type,
    from {{ ref('int_cfb_market_efficiency') }}
)

select
    game_id,
    bookmaker_key,
    sport_type,
    case
        when market_vig between 1.0 and 1.1 then 'Low Vig'
        when market_vig between 1.1 and 1.2 then 'Medium Vig'
        else 'High Vig'
    end as vig_category,
    market_favorite,
    case
        when probability_delta < 0.1 then 'Close'
        when probability_delta < 0.2 then 'Moderate'
        else 'Wide'
    end as market_spread_type,
    current_timestamp() as valid_from
from market_metrics
