-- Test to ensure odds are within reasonable range
with odds_validation as (
    select
        game_id,
        home_price,
        away_price
    from {{ ref('stg_nba_odds') }}
    where home_price < -100001 or home_price > 100001
    or away_price < -100001 or away_price > 100001
)
select * from odds_validation