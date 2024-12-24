{{ config(
    materialized='incremental',
    unique_key=['game_id', 'team_type'],
    schema='int_layer'
) }}

with team_performance as (
    select
        game_id,
        'HOME' as team_type,
        home_team_id,
        home_team_name,
        home_team_abbreviation,
        home_team_score as points_scored,
        home_team_q1_score as q1_points,
        home_team_q2_score as q2_points,
        home_team_q3_score as q3_points,
        home_team_q4_score as q4_points,
        home_team_record,
        case when home_team_score > away_team_score then 1 else 0 end as is_winner
    from {{ ref('stg_nfl_games') }}

    union all

    select
        game_id,
        'AWAY' as team_type,
        away_team_id,
        away_team_name,
        away_team_abbreviation,
        away_team_score as points_scored,
        away_team_q1_score as q1_points,
        away_team_q2_score as q2_points,
        away_team_q3_score as q3_points,
        away_team_q4_score as q4_points,
        away_team_record,
        case when away_team_score > home_team_score then 1 else 0 end as is_winner
    from {{ ref('stg_nfl_games') }}
)

select * from team_performance
