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
        VALUE:home_team.id::string as home_id,
        VALUE:home_team.name::string as home_name,
        VALUE:home_team.abbreviation::string as home_abbreviation,
        VALUE:home_team.score::integer as home_score,
        VALUE:home_team.record::string as home_record,
        VALUE:home_team.goals::integer as home_goals,
        VALUE:home_team.assists::integer as home_assists,
        VALUE:home_team.saves::integer as home_saves,
        VALUE:home_team.save_pct::float as home_save_pct,
        VALUE:home_team.points::integer as home_points,
        VALUE:home_team.linescores::array as home_linescores,
        -- Away Team
        VALUE:away_team.id::string as away_id,
        VALUE:away_team.name::string as away_name,
        VALUE:away_team.abbreviation::string as away_abbreviation,
        VALUE:away_team.score::integer as away_score,
        VALUE:away_team.record::string as away_record,
        VALUE:away_team.goals::integer as away_goals,
        VALUE:away_team.assists::integer as away_assists,
        VALUE:away_team.saves::integer as away_saves,
        VALUE:away_team.save_pct::float as away_save_pct,
        VALUE:away_team.points::integer as away_points,
        VALUE:away_team.linescores::array as away_linescores,
        -- leaders
        object_construct(
            'goals', object_construct(
                'name', VALUE:leaders.home_leaders.goals.name::string,
                'value', VALUE:leaders.home_leaders.goals.value::integer,
                'team', VALUE:leaders.home_leaders.goals.team::string
            ),
            'assists', object_construct(
                'name', VALUE:leaders.home_leaders.assists.name::string,
                'value', VALUE:leaders.home_leaders.assists.value::integer,
                'team', VALUE:leaders.home_leaders.assists.team::string
            ),
            'points', object_construct(
                'name', VALUE:leaders.home_leaders.points.name::string,
                'value', VALUE:leaders.home_leaders.points.value::integer,
                'team', VALUE:leaders.home_leaders.points.team::string
            )
        )::variant as home_leaders,
        object_construct(
            'goals', object_construct(
                'name', VALUE:leaders.away_leaders.goals.name::string,
                'value', VALUE:leaders.away_leaders.goals.value::integer,
                'team', VALUE:leaders.away_leaders.goals.team::string
            ),
            'assists', object_construct(
                'name', VALUE:leaders.away_leaders.assists.name::string,
                'value', VALUE:leaders.away_leaders.assists.value::integer,
                'team', VALUE:leaders.away_leaders.assists.team::string
            ),
            'points', object_construct(
                'name', VALUE:leaders.away_leaders.points.name::string,
                'value', VALUE:leaders.away_leaders.points.value::integer,
                'team', VALUE:leaders.away_leaders.points.team::string
            )
        )::variant as away_leaders,
        -- Venue
        VALUE:venue.name::string as venue_name,
        VALUE:venue.city::string as venue_city,
        VALUE:venue.state::string as venue_state,
        VALUE:venue.indoor::boolean as is_indoor,
        -- Broadcasts and Timestamp
        VALUE:broadcasts::array as broadcast,
        VALUE:ingestion_timestamp::timestamp as ingestion_timestamp
    from source
    where VALUE:start_time is not NULL
    qualify row_number() over (partition by VALUE:game_id order by VALUE:ingestion_timestamp desc nulls last) = 1
)
select * from staged
