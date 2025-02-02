{% macro create_overtime(sport, default_periods) %}
{{ config(
    materialized='incremental',
    unique_key='game_id',
    schema='int_layer',
    incremental_strategy='merge',
    cluster_by=['partition_year', 'partition_month', 'partition_day'],
    on_schema_change='append_new_columns',
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
            when array_size(home_linescores) > {{ default_periods }} then true
            else false
        end as went_to_overtime,
        case when array_size(home_linescores) - {{ default_periods }} > 0 then array_size(home_linescores) - {{ default_periods }}
            else 0
        end as number_of_overtimes,
        -- Overtime Scoring
        {{ get_overtime_scores('home_linescores', default_periods)}} as home_ot_scores,
        {{ get_overtime_scores('away_linescores', default_periods)}} as away_ot_scores,
        -- Winner in OT
        case
            when array_size(home_linescores) > {{ default_periods }} and home_score > away_score then 'HOME'
            when array_size(home_linescores) > {{ default_periods }}  and away_score > home_score then 'AWAY'
            else null
        end as ot_winner,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('stg_' ~ sport ~ '_games') }}
    where status_detail not in ('Postponed', 'Canceled') and
    total_periods > {{ default_periods }}
)
select * from overtime_analysis
{% if is_incremental() %}
where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
{% endif %}

{% endmacro %}