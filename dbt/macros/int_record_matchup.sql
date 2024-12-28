{% macro create_record_matchup(sport) %}
{{ config(
    materialized='incremental',
    unique_key='game_id',
    schema='int_layer',
    incremental_strategy='merge',
    alias='int_' ~ sport ~ '_record_match'
) }}

with record_analysis as (
    select
        game_id,
        -- Team Records
        home_name,
        home_record,
        regexp_substr(home_record, '[0-9]+')::int as home_wins,
        regexp_substr(home_record, '-([0-9]+)-')::int as home_losses,
        away_name,
        away_record,
        regexp_substr(away_record, '[0-9]+')::int as away_wins,
        regexp_substr(away_record, '-([0-9]+)-')::int as away_losses,
        -- Matchup Analysis
        case
            when regexp_substr(home_record, '[0-9]+')::float /
                 nullif(regexp_substr(home_record, '-([0-9]+)-')::float, 0) >
                 regexp_substr(away_record, '[0-9]+')::float /
                 nullif(regexp_substr(away_record, '-([0-9]+)-')::float, 0)
            then 'HOME_FAVORED'
            else 'AWAY_FAVORED'
        end as record_favorite,
        -- Actual Result
        case when home_score > away_score then 'HOME' else 'AWAY' end as winner,
        ingestion_timestamp
    from {{ ref('stg_' ~ sport ~ '_games') }}
    {% if is_incremental() %}
    where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
)
select * from record_analysis

{% endmacro %}