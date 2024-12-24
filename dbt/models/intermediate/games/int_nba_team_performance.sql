{{ config(
    materialized='incremental',
    unique_key=['game_id', 'team_type'],
    schema='int_layer'
) }}

with team_stats as (
    select
        game_id,
        'HOME' as team_type,
        home_team_name as team_name,
        cast(home_team_score as integer) as points_scored,
        cast(home_team_rebounds as integer) as rebounds,
        cast(home_team_assists as integer) as assists,
        cast(replace(home_team_fg_pct, '%', '') as float)/100 as fg_percentage,
        cast(replace(home_team_3p_pct, '%', '') as float)/100 as three_pt_percentage,
        cast(replace(home_team_ft_pct, '%', '') as float)/100 as ft_percentage,
        cast(home_team_assists as float)/nullif(cast(home_team_score as integer), 0) as assists_per_point
    from {{ ref('stg_nba_games') }}

    union all

    select
        game_id,
        'AWAY' as team_type,
        away_team_name as team_name,
        cast(away_team_score as integer) as points_scored,
        cast(away_team_rebounds as integer) as rebounds,
        cast(away_team_assists as integer) as assists,
        cast(replace(away_team_fg_pct, '%', '') as float)/100 as fg_percentage,
        cast(replace(away_team_3p_pct, '%', '') as float)/100 as three_pt_percentage,
        cast(replace(away_team_ft_pct, '%', '') as float)/100 as ft_percentage,
        cast(away_team_assists as float)/nullif(cast(away_team_score as integer), 0) as assists_per_point
    from {{ ref('stg_nba_games') }}
)
select * from team_stats
