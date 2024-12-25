{{ config(
    materialized='incremental',
    unique_key=['game_id', 'team_type'],
    schema='int_layer',
    incremental_strategy='merge',
    cluster_by=['partition_year', 'partition_month', 'partition_day']
) }}

with team_performance as (
    select
        game_id,
        'HOME' as team_type,
        home_id as team_id,
        home_name as team_name,
        home_abbreviation as team_abbreviation,
        home_score as points_scored,
        home_record,
        -- Scoring Analysis
        home_linescores[0]::integer + home_linescores[1]::integer as first_half_points,
        case when array_size(home_linescores) > 3 then home_linescores[2]::integer + home_linescores[3]::integer else home_linescores[2] end as second_half_points,
        case when home_score > away_score then 1 else 0 end as is_winner,
        case when array_size(home_linescores) > 4 then 1 else 0 end as played_overtime,
        partition_year,
        partition_month,
        partition_day
    from {{ ref('stg_cfb_games') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}

    union all

    select
        game_id,
        'AWAY' as team_type,
        away_id as team_id,
        away_name as team_name,
        away_abbreviation as team_abbreviation,
        away_score as points_scored,
        away_record,
        away_linescores[0]::integer + away_linescores[1]::integer as first_half_points,
        case when array_size(away_linescores) > 3 then away_linescores[2]::integer + away_linescores[3]::integer else away_linescores[2] end as second_half_points,
        case when away_score > home_score then 1 else 0 end as is_winner,
        case when array_size(away_linescores) > 4 then 1 else 0 end as played_overtime,
        partition_year,
        partition_month,
        partition_day
    from {{ ref('stg_cfb_games') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
)
select * from team_performance
