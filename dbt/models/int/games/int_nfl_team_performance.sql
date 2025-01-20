{{ config(
    materialized='incremental',
    unique_key=['game_id', 'team_type'],
    schema='int_layer',
    incremental_strategy='merge',
    cluster_by=['partition_year', 'partition_month', 'partition_day'],
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
        case when array_size(home_linescores) >= 2
            then home_linescores[0]::integer + coalesce(home_linescores[1]::integer, 0)
            else home_linescores[0]::integer
        end as first_half_points,
        case when array_size(home_linescores) >= 4
            then home_linescores[2]::integer + home_linescores[3]::integer
            else coalesce(home_linescores[2]::integer, 0)
        end as second_half_points,
        case when home_score > away_score then 1 else 0 end as is_winner,
        case when array_size(home_linescores) > 4 then 1 else 0 end as played_overtime,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('stg_nfl_games') }}
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
        case when array_size(away_linescores) >= 2
            then away_linescores[0]::integer + coalesce(away_linescores[1]::integer, 0)
            else away_linescores[0]::integer
        end as first_half_points,
        case when array_size(away_linescores) >= 4
            then away_linescores[2]::integer + away_linescores[3]::integer
            else coalesce(away_linescores[2]::integer, 0)
        end as second_half_points,
        case when away_score > home_score then 1 else 0 end as is_winner,
        case when array_size(away_linescores) > 4 then 1 else 0 end as played_overtime,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('stg_nfl_games') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
)
select * from team_performance