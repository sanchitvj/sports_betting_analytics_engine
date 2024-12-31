{{ config(
    materialized='table',  -- Use table since bookmaker data changes infrequently
    unique_key='bookmaker_key',
    schema='core'
) }}

with bookmakers_list as (
    select distinct
        bookmaker_key,
        bookmaker_title as bookmaker_name,
        'NBA' as sport_type
    from {{ ref('stg_nba_odds') }}

    union distinct

    select distinct
        bookmaker_key,
        bookmaker_title,
        'NFL' as sport_type
    from {{ ref('stg_nfl_odds') }}

    union distinct

    select distinct
        bookmaker_key,
        bookmaker_title,
        'NHL' as sport_type
    from {{ ref('stg_nhl_odds') }}

    union distinct

    select distinct
        bookmaker_key,
        bookmaker_title,
        'CFB' as sport_type
    from {{ ref('stg_cfb_odds') }}
)

select
    bookmaker_key,
    bookmaker_name,
    array_agg(distinct sport_type) as supported_sports,
    current_timestamp() as valid_from
from bookmakers_list
group by 1, 2
