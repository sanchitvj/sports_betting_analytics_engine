{{ config(
    materialized='table',
    unique_key='venue_key',
    schema='mart_core'
) }}

with venue_info as (
    select distinct
        venue_name,
        venue_city,
        venue_state,
        True as is_indoor,
        'NBA' as sport_type,
        md5(concat(
            coalesce(venue_name, ''),
            coalesce(venue_city, ''),
            coalesce(venue_state, '')
        )) as venue_key
    from {{ ref('int_nba_game_stats') }}

    union distinct

    select distinct
        venue_name,
        venue_city,
        venue_state,
        is_indoor,
        'NFL' as sport_type,
        md5(concat(
            coalesce(venue_name, ''),
            coalesce(venue_city, ''),
            coalesce(venue_state, '')
        )) as venue_key
    from {{ ref('int_nfl_game_stats') }}

    union distinct

    select distinct
        venue_name,
        venue_city,
        venue_state,
        is_indoor,
        'NHL' as sport_type,
        md5(concat(
            coalesce(venue_name, ''),
            coalesce(venue_city, ''),
            coalesce(venue_state, '')
        )) as venue_key
    from {{ ref('int_nhl_game_stats') }}

    union distinct

    select distinct
        venue_name,
        venue_city,
        venue_state,
        is_indoor,
        'CFB' as sport_type,
        md5(concat(
            coalesce(venue_name, ''),
            coalesce(venue_city, ''),
            coalesce(venue_state, '')
        )) as venue_key
    from {{ ref('int_cfb_game_stats') }}
)

select
    venue_key,
    venue_name,
    venue_city,
    venue_state,
    is_indoor,
    current_timestamp() as valid_from
from venue_info
where venue_name is not null
