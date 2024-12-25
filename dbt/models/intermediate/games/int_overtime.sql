{% macro create_overtime(sport, default_periods) %}
{{ config(
    materialized='incremental',
    unique_key='game_id',
    schema='int_layer',
    incremental_strategy='merge',
    alias='int_' ~ sport ~ '_overtime'
) }}

with overtime_analysis as (
    select
        game_id,
        home_name,
        away_name,
        -- Overtime Detection
        array_size(home_linescores) as total_periods,
        case
            when array_size(home_linescores) > default_periods then true
            else false
        end as went_to_overtime,
        array_size(home_linescores) - default_periods as number_of_overtimes,
        -- Overtime Scoring
        case when array_size(home_linescores) > default_periods
            then array_slice(home_linescores, default_periods, array_size(home_linescores)-1)
        end as home_ot_scores,
        case when array_size(away_linescores) > default_periods
            then array_slice(away_linescores, default_periods, array_size(away_linescores)-1)
        end as away_ot_scores,
        -- Winner in OT
        case
            when array_size(home_linescores) > default_periods and home_score > away_score then 'HOME'
            when array_size(home_linescores) > default_periods and away_score > home_score then 'AWAY'
            else null
        end as ot_winner
    from {{ ref('stg_' ~ sport ~ '_games') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
)
select * from overtime_analysis

{% endmacro %}

{{ create_overtime('cfb', 4) }}
{{ create_overtime('nba', 4) }}
{{ create_overtime('nfl', 4) }}
{{ create_overtime('nhl', 3) }}
