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
        VALUE:home_team.id::string as home_team_id,
        VALUE:home_team.name::string as home_team_name,
        VALUE:home_team.abbreviation::string as home_team_abbreviation,
        VALUE:home_team.score::integer as home_team_score,
        VALUE:home_team.record::string as home_team_record,
        VALUE:home_team.linescores[0]::integer as home_team_q1_score,
        VALUE:home_team.linescores[1]::integer as home_team_q2_score,
        VALUE:home_team.linescores[2]::integer as home_team_q3_score,
        VALUE:home_team.linescores[3]::integer as home_team_q4_score,
        -- Away Team
        VALUE:away_team.id::string as away_team_id,
        VALUE:away_team.name::string as away_team_name,
        VALUE:away_team.abbreviation::string as away_team_abbreviation,
        VALUE:away_team.score::integer as away_team_score,
        VALUE:away_team.record::string as away_team_record,
        VALUE:away_team.linescores[0]::integer as away_team_q1_score,
        VALUE:away_team.linescores[1]::integer as away_team_q2_score,
        VALUE:away_team.linescores[2]::integer as away_team_q3_score,
        VALUE:away_team.linescores[3]::integer as away_team_q4_score,
        -- Leaders
        VALUE:leaders.passing.name::string as passing_leader_name,
        VALUE:leaders.passing.display_value::string as passing_leader_stats,
        VALUE:leaders.passing.value::integer as passing_yards,
        VALUE:leaders.passing.team::string as passing_leader_team,
        VALUE:leaders.rushing.name::string as rushing_leader_name,
        VALUE:leaders.rushing.display_value::string as rushing_leader_stats,
        VALUE:leaders.rushing.value::integer as rushing_yards,
        VALUE:leaders.rushing.team::string as rushing_leader_team,
        VALUE:leaders.receiving.name::string as receiving_leader_name,
        VALUE:leaders.receiving.display_value::string as receiving_leader_stats,
        VALUE:leaders.receiving.value::integer as receiving_yards,
        VALUE:leaders.receiving.team::string as receiving_leader_team,
        -- Venue
        VALUE:venue_name::string as venue_name,
        VALUE:venue_city::string as venue_city,
        VALUE:venue_state::string as venue_state,
        VALUE:venue_indoor::boolean as is_indoor,
        VALUE:broadcasts::variant as broadcasts,
        VALUE:ingestion_timestamp::timestamp as ingestion_timestamp
    from source
    where VALUE:start_time is not NULL
    qualify row_number() over (partition by VALUE:game_id order by VALUE:ingestion_timestamp desc) = 1
)
select * from staged
