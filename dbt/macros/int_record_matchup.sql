{% macro create_record_matchup(sport) %}
{{ config(
    materialized='incremental',
    unique_key='game_id',
    schema='int_layer',
    incremental_strategy='merge',
    cluster_by=['partition_year', 'partition_month', 'partition_day'],
    on_schema_change='append_new_columns',
    alias='int_' ~ sport ~ '_record_match'
) }}

with record_analysis as (
    select
        game_id,
        -- Team Records
        home_name,
        home_record,
        try_cast(regexp_substr(home_record, '^[0-9]+') as int) as home_wins,
        try_cast(regexp_substr(home_record, '-([0-9]+)', 1, 1, 'e') as int) as home_losses,
        away_name,
        away_record,
        try_cast(regexp_substr(away_record, '^[0-9]+') as int) as away_wins,
        try_cast(regexp_substr(away_record, '-([0-9]+)', 1, 1, 'e') as int) as away_losses,
        -- Matchup Analysis
        case
            when try_cast(regexp_substr(home_record, '^[0-9]+') as float) /
                 nullif(try_cast(regexp_substr(home_record, '-([0-9]+)', 1, 1, 'e') as float), 0) >
                 try_cast(regexp_substr(away_record, '^[0-9]+') as float) /
                 nullif(try_cast(regexp_substr(away_record, '-([0-9]+)', 1, 1, 'e') as float), 0)
            then 'HOME_FAVORED'
            when try_cast(regexp_substr(home_record, '^[0-9]+') as float) /
                 nullif(try_cast(regexp_substr(home_record, '-([0-9]+)', 1, 1, 'e') as float), 0) <
                 try_cast(regexp_substr(away_record, '^[0-9]+') as float) /
                 nullif(try_cast(regexp_substr(away_record, '-([0-9]+)', 1, 1, 'e') as float), 0)
            then 'AWAY_FAVORED'
            else ''
        end as record_favorite,
        -- Actual Result
        case when home_score > away_score then 'HOME' else 'AWAY' end as winner,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('stg_' ~ sport ~ '_games') }}
    where home_record != '-'
    and away_record != '-'
    {% if is_incremental() %}
    and ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
)

select * from record_analysis

{% endmacro %}