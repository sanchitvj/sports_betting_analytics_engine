{{ config(
    materialized='incremental',
    unique_key='game_id',
    incremental_strategy='merge',
    cluster_by=['partition_year', 'partition_month', 'partition_day'],
    schema='silver_layer'
) }}

with source as (
    select VALUE from {{ source('raw_layer', 'nhl_games') }}
    {% if is_incremental() %}
    where VALUE:ingestion_timestamp::timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
),
staged as (
    select distinct
        VALUE:game_id::string as game_id,
        VALUE:start_time::timestamp as start_time,
        VALUE:partition_year::integer as partition_year,
        VALUE:partition_month::integer as partition_month,
        VALUE:partition_day::integer as partition_day,
        VALUE:status_state::string as status_state,
        VALUE:status_detail::string as status_detail,
        VALUE:status_description::string as status_description,
        VALUE:period::integer as period,
        VALUE:clock::string as clock,
        -- Home Team
        VALUE:home_team.id::string as home_team_id,
        VALUE:home_team.name::string as home_team_name,
        VALUE:home_team.abbreviation::string as home_team_abbreviation,
        VALUE:home_team.score::integer as home_team_score,
        VALUE:home_team.record::string as home_team_record,
        VALUE:home_team.goals::integer as home_team_goals,
        VALUE:home_team.assists::integer as home_team_assists,
        VALUE:home_team.saves::integer as home_team_saves,
        VALUE:home_team.save_pct::float as home_team_save_pct,
        VALUE:home_team.points::integer as home_team_points,
        -- Away Team
        VALUE:away_team.id::string as away_team_id,
        VALUE:away_team.name::string as away_team_name,
        VALUE:away_team.abbreviation::string as away_team_abbreviation,
        VALUE:away_team.score::integer as away_team_score,
        VALUE:away_team.record::string as away_team_record,
        VALUE:away_team.goals::integer as away_team_goals,
        VALUE:away_team.assists::integer as away_team_assists,
        VALUE:away_team.saves::integer as away_team_saves,
        VALUE:away_team.save_pct::float as away_team_save_pct,
        VALUE:away_team.points::integer as away_team_points,
        -- Venue
        VALUE:venue.name::string as venue_name,
        VALUE:venue.city::string as venue_city,
        VALUE:venue.state::string as venue_state,
        VALUE:venue.indoor::boolean as is_indoor,
        -- Broadcasts and Timestamp
        VALUE:broadcasts::variant as broadcast,
        VALUE:ingestion_timestamp::timestamp as ingestion_timestamp
    from source
    where VALUE:start_time is not NULL
    qualify row_number() over (partition by VALUE:game_id order by VALUE:ingestion_timestamp desc nulls last) = 1
)
select * from staged
