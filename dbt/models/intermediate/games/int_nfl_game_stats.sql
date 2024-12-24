{{ config(
    materialized='incremental',
    unique_key='game_id',
    schema='int_layer'
) }}

with game_stats as (
    select
        game_id,
        start_time,
        home_team_score,
        away_team_score,
        -- Quarter Analysis
        home_team_q1_score + home_team_q2_score as home_first_half_score,
        home_team_q3_score + home_team_q4_score as home_second_half_score,
        away_team_q1_score + away_team_q2_score as away_first_half_score,
        away_team_q3_score + away_team_q4_score as away_second_half_score,
        -- Score Differentials
        abs(home_team_score - away_team_score) as score_difference,
        case
            when home_team_score > away_team_score then 'HOME'
            else 'AWAY'
        end as winner,
        -- Venue Info
        venue_name,
        venue_city,
        venue_state,
        is_indoor
    from {{ ref('stg_nfl_games') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
)

select * from game_stats
