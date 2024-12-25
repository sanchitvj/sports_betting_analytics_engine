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
        abs(home_score - away_score) as goal_difference,
        case when home_score > away_score then 'HOME' else 'AWAY' end as winner,
        -- Period Analysis
        period as total_periods,
        -- Period Scoring (Safe Access)
        case when array_size(home_linescores) >= 1 then home_linescores[0] else null end as home_p1_score,
        case when array_size(home_linescores) >= 2 then home_linescores[1] else null end as home_p2_score,
        case when array_size(home_linescores) >= 3 then home_linescores[2] else null end as home_p3_score,
        case when array_size(away_linescores) >= 1 then away_linescores[0] else null end as away_p1_score,
        case when array_size(away_linescores) >= 2 then away_linescores[1] else null end as away_p2_score,
        case when array_size(away_linescores) >= 3 then away_linescores[2] else null end as away_p3_score,
        -- Overtime Handling
        array_size(home_linescores) - 4 as number_of_overtimes,
        case when array_size(home_linescores) > 3
            then array_slice(home_linescores, 3, array_size(home_linescores)-1)
        end as home_ot_scores,
        case when array_size(away_linescores) > 3
            then array_slice(away_linescores, 3, array_size(away_linescores)-1)
        end as away_ot_scores,
        -- Game Totals
        home_goals + away_goals as total_goals,
        home_assists + away_assists as total_assists,
        home_saves + away_saves as total_saves,
        -- Venue Info
        venue_name,
        venue_city,
        venue_state,
        is_indoor,
        partition_year,
        partition_month,
        partition_day
    from {{ ref('stg_nhl_games') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
)
select * from game_stats
