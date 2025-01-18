{{ config(
    materialized='table',
    unique_key='team_key',
    schema='mart_core'
) }}

with nba_teams as (
    select distinct
        t.team_id,
        t.team_name,
        t.team_abbreviation,
        'NBA' as sport_type,
        g.venue_city as city,
        g.venue_state as state,
        md5(concat(
            coalesce(t.team_name, ''),
            coalesce(t.team_abbreviation, ''),
            coalesce(g.venue_city, ''),
            coalesce(g.venue_state, '')
        )) as team_key
    from {{ ref('int_nba_team_performance') }} t
    left join {{ ref('int_nba_game_stats') }} g
        on t.game_id = g.game_id
),

nfl_teams as (
    select distinct
        t.team_id,
        t.team_name,
        t.team_abbreviation,
        'NFL' as sport_type,
        g.venue_city as city,
        g.venue_state as state,
        md5(concat(
            coalesce(t.team_name, ''),
            coalesce(t.team_abbreviation, ''),
            coalesce(g.venue_city, ''),
            coalesce(g.venue_state, '')
        )) as team_key
    from {{ ref('int_nfl_team_performance') }} t
    left join {{ ref('int_nfl_game_stats') }} g
        on t.game_id = g.game_id
),

nhl_teams as (
    select distinct
        t.team_id,
        t.team_name,
        t.team_abbreviation,
        'NHL' as sport_type,
        g.venue_city as city,
        g.venue_state as state,
        md5(concat(
            coalesce(t.team_name, ''),
            coalesce(t.team_abbreviation, ''),
            coalesce(g.venue_city, ''),
            coalesce(g.venue_state, '')
        )) as team_key
    from {{ ref('int_nhl_team_performance') }} t
    left join {{ ref('int_nhl_game_stats') }} g
        on t.game_id = g.game_id
),

cfb_teams as (
    select distinct
        t.team_id,
        t.team_name,
        t.team_abbreviation,
        'CFB' as sport_type,
        g.venue_city as city,
        g.venue_state as state,
        md5(concat(
            coalesce(t.team_name, ''),
            coalesce(t.team_abbreviation, ''),
            coalesce(g.venue_city, ''),
            coalesce(g.venue_state, '')
        )) as team_key
    from {{ ref('int_cfb_team_performance') }} t
    left join {{ ref('int_cfb_game_stats') }} g
        on t.game_id = g.game_id
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
where team_id is not null