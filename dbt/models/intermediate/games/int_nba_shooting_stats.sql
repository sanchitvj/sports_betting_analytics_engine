{{ config(
    materialized='incremental',
    unique_key=['game_id', 'team_type'],
    schema='intermediate'
) }}

with shooting_analysis as (
    select
        game_id,
        start_time,
        'HOME' as team_type,
        home_team_name as team_name,
        cast(replace(home_team_fg_pct, '%', '') as float)/100 as fg_percentage,
        cast(replace(home_team_3p_pct, '%', '') as float)/100 as three_pt_percentage,
        cast(replace(home_team_ft_pct, '%', '') as float)/100 as ft_percentage,
        cast(home_team_score as integer) as total_points,
        round(cast(home_team_score as integer) * cast(replace(home_team_3p_pct, '%', '') as float)/100 * 0.3) as estimated_three_points,
        round(cast(home_team_score as integer) * cast(replace(home_team_ft_pct, '%', '') as float)/100 * 0.2) as estimated_free_throws
    from {{ ref('stg_nba_games') }}

    union all

    select
        game_id,
        start_time,
        'AWAY' as team_type,
        away_team_name as team_name,
        cast(replace(away_team_fg_pct, '%', '') as float)/100 as fg_percentage,
        cast(replace(away_team_3p_pct, '%', '') as float)/100 as three_pt_percentage,
        cast(replace(away_team_ft_pct, '%', '') as float)/100 as ft_percentage,
        cast(away_team_score as integer) as total_points,
        round(cast(away_team_score as integer) * cast(replace(away_team_3p_pct, '%', '') as float)/100 * 0.3) as estimated_three_points,
        round(cast(away_team_score as integer) * cast(replace(away_team_ft_pct, '%', '') as float)/100 * 0.2) as estimated_free_throws
    from {{ ref('stg_nba_games') }}
)
select * from shooting_analysis
