{{ config(
    materialized='view',
    schema='silver_layer'
) }}

with source as (
    select DATA from {{ source('raw_data', 'nba_games') }}
),
standardized as (
    select
        DATA:game_id::string as game_id,
        DATA:partition_year::integer as partition_year,
        DATA:partition_month::integer as partition_month,
        DATA:partition_day::integer as partition_day,
        DATA:status_state::string as status_state,
        DATA:status_detail::string as status_detail,
        DATA:status_description::string as status_description,
        DATA:period::integer as period,
        DATA:clock::string as clock,
        -- Home Team
        DATA:home_team:name::string as home_team_name,
        DATA:home_team:score::string as home_team_score,
        DATA:home_team:rebounds::string as home_team_rebounds,
        DATA:home_team:assists::string as home_team_assists,
        DATA:home_team:field_goals::string as home_team_fg_pct,
        DATA:home_team:three_pointers::string as home_team_3p_pct,
        DATA:home_team:free_throws::string as home_team_ft_pct,
        -- Away Team
        DATA:away_team:name::string as away_team_name,
        DATA:away_team:score::string as away_team_score,
        DATA:away_team:rebounds::string as away_team_rebounds,
        DATA:away_team:assists::string as away_team_assists,
        DATA:away_team:field_goals::string as away_team_fg_pct,
        DATA:away_team:three_pointers::string as away_team_3p_pct,
        DATA:away_team:free_throws::string as away_team_ft_pct,
        -- Venue
        DATA:venue:name::string as venue_name,
        DATA:venue:city::string as venue_city,
        DATA:venue:state::string as venue_state,
        DATA:ingestion_timestamp::timestamp as ingestion_timestamp,
        DATA:broadcasts::variant as broadcasts
    from source
)
select * from standardized