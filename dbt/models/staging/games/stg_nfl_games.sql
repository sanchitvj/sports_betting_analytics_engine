{{ config(
    materialized='incremental',
    unique_key='game_id',
    incremental_strategy='merge',
    cluster_by=['partition_year', 'partition_month', 'partition_day'],
    schema='silver_layer',
) }}

with source as (
    select VALUE from {{ source('raw_layer', 'nfl_games') }}
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
        VALUE:home_team.id::string as home_id,
        VALUE:home_team.name::string as home_name,
        VALUE:home_team.abbreviation::string as home_abbreviation,
        VALUE:home_team.score::integer as home_score,
        VALUE:home_team.record::string as home_record,
        VALUE:home_team.linescores::array as home_linescores,
        -- Away Team
        VALUE:away_team.id::string as away_id,
        VALUE:away_team.name::string as away_name,
        VALUE:away_team.abbreviation::string as away_abbreviation,
        VALUE:away_team.score::integer as away_score,
        VALUE:away_team.record::string as away_record,
        VALUE:away_team.linescores::array as away_linescores,
        -- Leaders
        object_construct(
            'PASSING', object_construct(
                'name', VALUE:leaders.passing.name::string,
                'display_value', VALUE:leaders.passing.display_value::string,
                'value', VALUE:leaders.passing.value::integer,
                'team', VALUE:leaders.passing.team::string
            ),
            'RUSHING', object_construct(
                'name', VALUE:leaders.rushing.name::string,
                'display_value', VALUE:leaders.rushing.display_value::string,
                'value', VALUE:leaders.rushing.value::integer,
                'team', VALUE:leaders.rushing.team::string
            ),
            'RECEIVING', object_construct(
                'name', VALUE:leaders.receiving.name::string,
                'display_value', VALUE:leaders.receiving.display_value::string,
                'value', VALUE:leaders.receiving.value::integer,
                'team', VALUE:leaders.receiving.team::string
            )
        )::variant as game_leaders,
        -- Venue
        VALUE:venue.name::string as venue_name,
        VALUE:venue.city::string as venue_city,
        VALUE:venue.state::string as venue_state,
        VALUE:venue.indoor::boolean as is_indoor,
        VALUE:broadcasts::array as broadcasts,
        VALUE:ingestion_timestamp::timestamp as ingestion_timestamp
    from source
    where VALUE:start_time is not NULL
    qualify row_number() over (partition by VALUE:game_id order by VALUE:ingestion_timestamp desc) = 1
)
select * from staged
