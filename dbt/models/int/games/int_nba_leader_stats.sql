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
        'POINTS' as leader_type,
        home_leaders:points.name::string as player_name,
        home_leaders:points.value::integer as stat_value,
        home_leaders:points.value::string as display_value,
        home_leaders:points.team::string as team_id,
        partition_year, partition_month, partition_day,
        ingestion_timestamp
    from {{ ref('stg_nba_games') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}

    union all

    select
        game_id,
        'HOME' as team_type,
        'REBOUNDS' as leader_type,
        home_leaders:rebounds.name::string as player_name,
        home_leaders:rebounds.value::integer as stat_value,
        home_leaders:rebounds.value::string as display_value,
        home_leaders:rebounds.team::string as team_id,
        partition_year, partition_month, partition_day,
        ingestion_timestamp
    from {{ ref('stg_nba_games') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}

    union all

    select
        game_id,
        'HOME' as team_type,
        'ASSISTS' as leader_type,
        home_leaders:assists.name::string as player_name,
        home_leaders:assists.value::integer as stat_value,
        home_leaders:assists.value::string as display_value,
        home_leaders:assists.team::string as team_id,
        partition_year, partition_month, partition_day,
        ingestion_timestamp
    from {{ ref('stg_nba_games') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}

    union all

    select
        game_id,
        'HOME' as team_type,
        'RATING' as leader_type,
        home_leaders:rating.name::string as player_name,
        home_leaders:rating.value::integer as stat_value,
        home_leaders:rating.display_value:string as display_value,
        home_leaders:rating.team::string as team_id,
        partition_year, partition_month, partition_day,
        ingestion_timestamp
    from {{ ref('stg_nba_games') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}

    union all

    select
        game_id,
        'AWAY' as team_type,
        'POINTS' as leader_type,
        away_leaders:points.name::string as player_name,
        away_leaders:points.value::integer as stat_value,
        away_leaders:points.value::string as display_value,
        away_leaders:points.team::string as team_id,
        partition_year, partition_month, partition_day,
        ingestion_timestamp
    from {{ ref('stg_nba_games') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}

    union all

    select
        game_id,
        'AWAY' as team_type,
        'REBOUNDS' as leader_type,
        away_leaders:rebounds.name::string as player_name,
        away_leaders:rebounds.value::integer as stat_value,
        away_leaders:rebounds.value::string as display_value,
        away_leaders:rebounds.team::string as team_id,
        partition_year, partition_month, partition_day,
        ingestion_timestamp
    from {{ ref('stg_nba_games') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}

    union all

    select
        game_id,
        'AWAY' as team_type,
        'ASSISTS' as leader_type,
        away_leaders:assists.name::string as player_name,
        away_leaders:assists.value::integer as stat_value,
        away_leaders:assists.value::string as display_value,
        away_leaders:assists.team::string as team_id,
        partition_year, partition_month, partition_day,
        ingestion_timestamp
    from {{ ref('stg_nba_games') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}

    union all

    select
        game_id,
        'AWAY' as team_type,
        'RATING' as leader_type,
        away_leaders:rating.name::string as player_name,
        away_leaders:rating.value::integer as stat_value,
        away_leaders:rating.display_value:string as display_value,
        away_leaders:rating.team::string as team_id,
        partition_year, partition_month, partition_day,
        ingestion_timestamp
    from {{ ref('stg_nba_games') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
)
select * from leader_stats
