{{ config(
    materialized='table',
    unique_key='bookmaker_key',
    schema='mart_core'
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
    array_construct_compact(array_agg(distinct sport_type)) as supported_sports,
    current_timestamp() as valid_from
from bookmakers_list
group by 1, 2
