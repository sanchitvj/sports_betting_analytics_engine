{{ config(
    materialized='incremental',
    unique_key=['game_id', 'sport_type'],
    schema='mart_analytics',
    cluster_by=['partition_year', 'partition_month', 'partition_day'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

with overtime_metrics as (
    select
        game_id,
        'NBA' as sport_type,
        home_name,
        away_name,
        total_periods,
        went_to_overtime,
        number_of_overtimes,
        home_ot_scores,
        away_ot_scores,
        ot_winner,
        -- NBA-specific OT metrics
        array_size(home_ot_scores) * 5 as ot_duration_minutes,
        reduce(home_ot_scores, 0, (acc, val) -> acc + val) + reduce(away_ot_scores, 0, (acc, val) -> acc + val) as total_ot_points,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_nba_overtime') }}

    union all

    select
        game_id,
        'NFL' as sport_type,
        home_name,
        away_name,
        total_periods,
        went_to_overtime,
        number_of_overtimes,
        home_ot_scores,
        away_ot_scores,
        ot_winner,
        array_size(home_ot_scores) * 15 as ot_duration_minutes,
        reduce(home_ot_scores, 0, (acc, val) -> acc + val) + reduce(away_ot_scores, 0, (acc, val) -> acc + val) as total_ot_points,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_nfl_overtime') }}

    union all

    select
        game_id,
        'NHL' as sport_type,
        home_name,
        away_name,
        total_periods,
        went_to_overtime,
        number_of_overtimes,
        home_ot_scores,
        away_ot_scores,
        ot_winner,
        array_size(home_ot_scores) * 20 as ot_duration_minutes,
        reduce(home_ot_scores, 0, (acc, val) -> acc + val) + reduce(away_ot_scores, 0, (acc, val) -> acc + val) as total_ot_points,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_nhl_overtime') }}

    union all

    select
        game_id,
        'CFB' as sport_type,
        home_name,
        away_name,
        total_periods,
        went_to_overtime,
        number_of_overtimes,
        home_ot_scores,
        away_ot_scores,
        ot_winner,
        array_size(home_ot_scores) * 15 as ot_duration_minutes,
        reduce(home_ot_scores, 0, (acc, val) -> acc + val) + reduce(away_ot_scores, 0, (acc, val) -> acc + val) as total_ot_points,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_cfb_overtime') }}
),

aggregated_metrics as (
    select
        *,
        -- Team Performance in OT
        case
            when ot_winner = home_name then 'Won OT at Home'
            when ot_winner = away_name then 'Won OT Away'
            else null
        end as ot_win_type,

        -- Overtime Intensity
        case
            when sport_type in ('NBA', 'NFL', 'CFB') then
                case
                    when total_ot_points <= 10 then 'Low Scoring OT'
                    when total_ot_points <= 20 then 'Medium Scoring OT'
                    else 'High Scoring OT'
                end
            else -- NHL
                case
                    when total_ot_points = 1 then 'Quick OT'
                    else 'Extended OT'
                end
        end as ot_intensity
    from overtime_metrics
    where went_to_overtime = true
)

select * from aggregated_metrics
{% if is_incremental() %}
where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
{% endif %}
