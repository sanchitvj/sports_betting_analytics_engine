{{ config(
    materialized='incremental',
    unique_key=['game_id', 'player_name'],
    schema='int_layer'
) }}

with player_stats as (
    select
        game_id,
        passing_leader_name as player_name,
        'PASSING' as stat_type,
        passing_leader_team as team_id,
        passing_yards as yards,
        passing_leader_stats as display_stats
    from {{ ref('stg_nfl_games') }}
    where passing_leader_name is not null

    union all

    select
        game_id,
        rushing_leader_name as player_name,
        'RUSHING' as stat_type,
        rushing_leader_team as team_id,
        rushing_yards as yards,
        rushing_leader_stats as display_stats
    from {{ ref('stg_nfl_games') }}
    where rushing_leader_name is not null

    union all

    select
        game_id,
        receiving_leader_name as player_name,
        'RECEIVING' as stat_type,
        receiving_leader_team as team_id,
        receiving_yards as yards,
        receiving_leader_stats as display_stats
    from {{ ref('stg_nfl_games') }}
    where receiving_leader_name is not null
)

select * from player_stats
