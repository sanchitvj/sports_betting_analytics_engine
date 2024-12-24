{{ config(
    materialized='incremental',
    unique_key='game_id',
    schema='intermediate'
) }}

with game_stats as (
    select
        game_id,
        start_time,
        cast(home_team_score as integer) as home_score,
        cast(away_team_score as integer) as away_score,
        cast(home_team_rebounds as integer) + cast(away_team_rebounds as integer) as total_rebounds,
        cast(home_team_assists as integer) + cast(away_team_assists as integer) as total_assists,
        abs(cast(home_team_score as integer) - cast(away_team_score as integer)) as score_difference,
        case
            when cast(home_team_score as integer) > cast(away_team_score as integer) then 'HOME'
            else 'AWAY'
        end as winner,
        period,
        venue_city,
        venue_state
    from {{ ref('stg_nba_games') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
)
select * from game_stats
