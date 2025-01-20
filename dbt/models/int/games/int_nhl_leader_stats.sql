{{ config(
    materialized='incremental',
    unique_key=['game_id', 'leader_type', 'team_type'],
    schema='int_layer',
    incremental_strategy='merge',
    cluster_by=['partition_year', 'partition_month', 'partition_day']
) }}

with leader_stats as (
    select
        game_id,
        'HOME' as team_type,
        'GOALS' as leader_type,
        home_leaders:goals.name::string as player_name,
        home_leaders:goals.value::integer as stat_value,
        home_leaders:goals.team::string as team_id,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('stg_nhl_games') }}
    where status_detail not in ('Postponed', 'Canceled')

    union all

    select
        game_id,
        'HOME' as team_type,
        'ASSISTS' as leader_type,
        home_leaders:assists.name::string as player_name,
        home_leaders:assists.value::integer as stat_value,
        home_leaders:assists.team::string as team_id,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('stg_nhl_games') }}
    where status_detail not in ('Postponed', 'Canceled')

    union all

    select
        game_id,
        'HOME' as team_type,
        'POINTS' as leader_type,
        home_leaders:points.name::string as player_name,
        home_leaders:points.value::integer as stat_value,
        home_leaders:points.team::string as team_id,
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
        'GOALS' as leader_type,
        away_leaders:goals.name::string as player_name,
        away_leaders:goals.value::integer as stat_value,
        away_leaders:goals.team::string as team_id,
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
        'ASSISTS' as leader_type,
        away_leaders:assists.name::string as player_name,
        away_leaders:assists.value::integer as stat_value,
        away_leaders:assists.team::string as team_id,
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
        'POINTS' as leader_type,
        away_leaders:points.name::string as player_name,
        away_leaders:points.value::integer as stat_value,
        away_leaders:points.team::string as team_id,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('stg_nhl_games') }}
    where status_detail not in ('Postponed', 'Canceled')
)
select * from leader_stats
{% if is_incremental() %}
where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
{% endif %}