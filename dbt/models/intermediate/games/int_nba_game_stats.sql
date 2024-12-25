{{ config(
    materialized='incremental',
    unique_key='game_id',
    schema='int_layer',
    incremental_strategy='merge',
    cluster_by=['partition_year', 'partition_month', 'partition_day']
) }}

with game_stats as (
    select
        game_id,
        start_time,
        -- Game Metrics
        home_score,
        away_score,
        abs(home_score - away_score) as score_difference,
        case when home_score > away_score then 'HOME' else 'AWAY' end as winner,
        period as total_periods
        -- Total Game Stats
        home_rebounds + away_rebounds as total_rebounds,
        home_assists + away_assists as total_assists,
        -- Quarter Analysis
        case when array_size(home_linescores) >=0  then home_linescores[0] else null end as home_q1_score,
        case when array_size(home_linescores) >=1  then home_linescores[1] else null end as home_q2_score,
        case when array_size(home_linescores) >=2  then home_linescores[2] else null end as home_q3_score,
        case when array_size(home_linescores) >=3  then home_linescores[3] else null end as home_q4_score,
        case when array_size(away_linescores) >=0  then away_linescores[0] else null end as away_q1_score,
        case when array_size(away_linescores) >=1  then away_linescores[1] else null end as away_q2_score,
        case when array_size(away_linescores) >=2  then away_linescores[2] else null end as away_q3_score,
        case when array_size(away_linescores) >=3  then away_linescores[3] else null end as away_q4_score,
        -- Overtime handling
        array_size(home_linescores) - 4 as number_of_overtimes,
        case when array_size(home_linescores) > 4
            then array_agg(home_linescores[i])::array
            for i in 4..array_size(home_linescores)-1
        end as home_ot_scores,
        case when array_size(away_linescores) > 4
            then array_agg(away_linescores[i])::array
            for i in 4..array_size(away_linescores)-1
        end as away_ot_scores,
        -- Venue Info
        venue_name,
        venue_city,
        venue_state,
        partition_year,
        partition_month,
        partition_day
    from {{ ref('stg_nba_games') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
)
select * from game_stats
