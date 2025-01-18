{{ config(
    materialized='incremental',
    unique_key=['game_id', 'bookmaker_key', 'bookmaker_last_update'],
    schema='mart_analytics',
    incremental_strategy='merge',
    cluster_by=['partition_year', 'partition_month', 'partition_day']
) }}

with value_analysis as (
    select
        o.game_id,
        o.bookmaker_key,
        m.bookmaker_last_update,
        'NBA' as sport_type,
        -- Price Analysis
        o.opening_home_price,
        o.opening_away_price,
        o.closing_home_price,
        o.closing_away_price,
        -- Market Efficiency
        round(m.market_vig, 3) as market_vig,
        -- Value Indicators
        case
            when o.closing_home_price > o.opening_home_price + 15 then 'Strong Home Value'
            when o.closing_home_price > o.opening_home_price + 5 then 'Moderate Home Value'
            when o.closing_away_price > o.opening_away_price + 15 then 'Strong Away Value'
            when o.closing_away_price > o.opening_away_price + 5 then 'Moderate Away Value'
            else 'No Clear Value'
        end as value_direction,
        -- Steam Move Detection
        case
            when abs(o.home_price_movement) >= 10
            and m.market_vig <= 1.05 then true
            else false
        end as is_sharp_move,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_nba_odds_movement') }} o
    left join {{ ref('int_nba_market_efficiency') }} m
    using (game_id, bookmaker_key, bookmaker_last_update)

    union all

    select
        o.game_id,
        o.bookmaker_key,
        m.bookmaker_last_update,
        'NFL' as sport_type,
        o.opening_home_price,
        o.opening_away_price,
        o.closing_home_price,
        o.closing_away_price,
        round(m.market_vig, 3) as market_vig,
        case
            when o.closing_home_price > o.opening_home_price + 10 then 'Strong Home Value'
            when o.closing_home_price > o.opening_home_price + 3 then 'Moderate Home Value'
            when o.closing_away_price > o.opening_away_price + 10 then 'Strong Away Value'
            when o.closing_away_price > o.opening_away_price + 3 then 'Moderate Away Value'
            else 'No Clear Value'
        end as value_direction,
        case
            when abs(o.home_price_movement) >= 7
            and m.market_vig <= 1.05 then true
            else false
        end as is_sharp_move,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_nfl_odds_movement') }} o
    left join {{ ref('int_nfl_market_efficiency') }} m
    using (game_id, bookmaker_key, bookmaker_last_update)

    union all

    select
        o.game_id,
        o.bookmaker_key,
        m.bookmaker_last_update,
        'NHL' as sport_type,
        o.opening_home_price,
        o.opening_away_price,
        o.closing_home_price,
        o.closing_away_price,
        round(m.market_vig, 3) as market_vig,
        case
            when o.closing_home_price > o.opening_home_price + 20 then 'Strong Home Value'
            when o.closing_home_price > o.opening_home_price + 10 then 'Moderate Home Value'
            when o.closing_away_price > o.opening_away_price + 20 then 'Strong Away Value'
            when o.closing_away_price > o.opening_away_price + 10 then 'Moderate Away Value'
            else 'No Clear Value'
        end as value_direction,
        case
            when abs(o.home_price_movement) >= 15
            and m.market_vig <= 1.05 then true
            else false
        end as is_sharp_move,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_nhl_odds_movement') }} o
    left join {{ ref('int_nhl_market_efficiency') }} m
    using (game_id, bookmaker_key, bookmaker_last_update)

    union all

    select
        o.game_id,
        o.bookmaker_key,
        m.bookmaker_last_update,
        'CFB' as sport_type,
        o.opening_home_price,
        o.opening_away_price,
        o.closing_home_price,
        o.closing_away_price,
        round(m.market_vig, 3) as market_vig,
        case
            when o.closing_home_price > o.opening_home_price + 10 then 'Strong Home Value'
            when o.closing_home_price > o.opening_home_price + 3 then 'Moderate Home Value'
            when o.closing_away_price > o.opening_away_price + 10 then 'Strong Away Value'
            when o.closing_away_price > o.opening_away_price + 3 then 'Moderate Away Value'
            else 'No Clear Value'
        end as value_direction,
        case
            when abs(o.home_price_movement) >= 7
            and m.market_vig <= 1.05 then true
            else false
        end as is_sharp_move,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_cfb_odds_movement') }} o
    left join {{ ref('int_cfb_market_efficiency') }} m
    using (game_id, bookmaker_key, bookmaker_last_update)
)

select distinct * from value_analysis
-- qualify row_number() over (
--     partition by game_id, bookmaker_key, bookmaker_last_update
--     order by ingestion_timestamp desc
-- ) = 1
{% if is_incremental() %}
where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
{% endif %}
