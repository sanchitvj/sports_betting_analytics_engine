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
        home_score as goals_scored,
        home_assists as assists,
        home_saves as saves,
        home_save_pct as save_pct,
        home_points as points,
        -- Calculated Metrics
        cast(home_assists as float)/nullif(home_goals, 0) as assists_per_goal,
        cast(home_saves as float)/nullif(home_saves + away_goals, 0) as actual_save_pct,
        case when home_score > away_score then 1 else 0 end as is_winner,
        case when array_size(home_linescores) > 3 then 1 else 0 end as played_overtime,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('stg_nhl_games') }}
    where status_detail not in ('Postponed', 'Canceled')

    union all

    select
        game_id,
        'AWAY' as team_type,
        away_id as team_id,
        away_name as team_name,
        away_abbreviation as team_abbreviation,
        away_score as goals_scored,
        away_assists as assists,
        away_saves as saves,
        away_save_pct as save_pct,
        away_points as points,
        cast(away_assists as float)/nullif(away_goals, 0) as assists_per_goal,
        cast(away_saves as float)/nullif(away_saves + home_goals, 0) as actual_save_pct,
        case when away_score > home_score then 1 else 0 end as is_winner,
        case when array_size(away_linescores) > 3 then 1 else 0 end as played_overtime,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('stg_nhl_games') }}
    where status_detail not in ('Postponed', 'Canceled')
)
select * from team_performance
{% if is_incremental() %}
where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
{% endif %}