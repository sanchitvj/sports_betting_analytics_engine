{{ config(
    materialized='incremental',
    unique_key=['game_id', 'leader_type'],
    schema='int_layer',
    incremental_strategy='merge',
    cluster_by=['partition_year', 'partition_month', 'partition_day'],
) }}

with leader_stats as (
    select
        game_id,
        'PASSING' as leader_type,
        game_leaders:PASSING.name::string as player_name,
        game_leaders:PASSING.value::integer as stat_value,
        game_leaders:PASSING.display_value::string as display_value,
        game_leaders:PASSING.team::string as team_id,
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
        'RUSHING' as leader_type,
        game_leaders:RUSHING.name::string as player_name,
        game_leaders:RUSHING.value::integer as stat_value,
        game_leaders:RUSHING.display_value::string as display_value,
        game_leaders:RUSHING.team::string as team_id,
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
        'RECEIVING' as leader_type,
        game_leaders:RECEIVING.name::string as player_name,
        game_leaders:RECEIVING.value::integer as stat_value,
        game_leaders:RECEIVING.display_value::string as display_value,
        game_leaders:RECEIVING.team::string as team_id,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('stg_nfl_games') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
)
select * from leader_stats