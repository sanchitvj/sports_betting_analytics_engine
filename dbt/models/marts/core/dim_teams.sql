{{ config(
    materialized='table',  -- Using table since team data changes infrequently
    unique_key='team_key',
    schema='core'
) }}

with nba_teams as (
    select distinct
        team_id,
        team_name,
        team_abbreviation,
        'NBA' as sport_type,
        venue_city as city,
        venue_state as state,
        md5(concat(
            coalesce(team_name, ''),
            coalesce(team_abbreviation, ''),
            coalesce(city, ''),
            coalesce(state, '')
        )) as team_key
    from {{ ref('int_nba_team_performance') }}
),

nfl_teams as (
    select distinct
        team_id,
        team_name,
        team_abbreviation,
        'NFL' as sport_type,
        venue_city as city,
        venue_state as state,
        md5(concat(
            coalesce(team_name, ''),
            coalesce(team_abbreviation, ''),
            coalesce(city, ''),
            coalesce(state, '')
        )) as team_key
    from {{ ref('int_nfl_team_performance') }}
),

nhl_teams as (
    select distinct
        team_id,
        team_name,
        team_abbreviation,
        'NHL' as sport_type,
        venue_city as city,
        venue_state as state,
        md5(concat(
            coalesce(team_name, ''),
            coalesce(team_abbreviation, ''),
            coalesce(city, ''),
            coalesce(state, '')
        )) as team_key
    from {{ ref('int_nhl_team_performance') }}
),

cfb_teams as (
    select distinct
        team_id,
        team_name,
        team_abbreviation,
        'CFB' as sport_type,
        venue_city as city,
        venue_state as state,
        md5(concat(
            coalesce(team_name, ''),
            coalesce(team_abbreviation, ''),
            coalesce(city, ''),
            coalesce(state, '')
        )) as team_key
    from {{ ref('int_cfb_team_performance') }}
),

final as (
    select * from nba_teams
    union all
    select * from nfl_teams
    union all
    select * from nhl_teams
    union all
    select * from cfb_teams
)

select
    team_key,  -- For tracking changes
    team_id,
    team_name,
    team_abbreviation,
    sport_type,
    city,
    state,
    current_timestamp() as valid_from,
from final
