{{ config(
    materialized='incremental',
    unique_key='game_id',
    incremental_strategy='merge',
    cluster_by=['partition_year', 'partition_month', 'partition_day'],
    schema='silver_layer'
) }}

with source as (
    select VALUE from {{ source('raw_layer', 'nba_games') }}
    {% if is_incremental() %}
    where VALUE:ingestion_timestamp::timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
),
staged as (
    select distinct
        VALUE:game_id::string as game_id,
        VALUE:partition_year::integer as partition_year,
        VALUE:partition_month::integer as partition_month,
        VALUE:partition_day::integer as partition_day,
        VALUE:start_time::timestamp as start_time,
        VALUE:status_state::string as status_state,
        VALUE:status_detail::string as status_detail,
        VALUE:status_description::string as status_description,
        VALUE:period::integer as period,
        VALUE:clock::string as clock,
        -- Home Team
        VALUE:home_team.name::string as home_team_name,
        VALUE:home_team.score::string as home_team_score,
        VALUE:home_team.rebounds::string as home_team_rebounds,
        VALUE:home_team.assists::string as home_team_assists,
        VALUE:home_team.field_goals::string as home_team_fg_pct,
        VALUE:home_team.three_pointers::string as home_team_3p_pct,
        VALUE:home_team.free_throws::string as home_team_ft_pct,
        -- Away Team
        VALUE:away_team.name::string as away_team_name,
        VALUE:away_team.score::string as away_team_score,
        VALUE:away_team.rebounds::string as away_team_rebounds,
        VALUE:away_team.assists::string as away_team_assists,
        VALUE:away_team.field_goals::string as away_team_fg_pct,
        VALUE:away_team.three_pointers::string as away_team_3p_pct,
        VALUE:away_team.free_throws::string as away_team_ft_pct,
        -- Venue
        VALUE:venue.name::string as venue_name,
        VALUE:venue.city::string as venue_city,
        VALUE:venue.state::string as venue_state,
        VALUE:ingestion_timestamp::timestamp as ingestion_timestamp,
        VALUE:broadcasts::variant as broadcasts
    from source
    where VALUE:start_time is not NULL
    qualify row_number() over (partition by VALUE:game_id order by VALUE:ingestion_timestamp desc) = 1
)
select * from staged