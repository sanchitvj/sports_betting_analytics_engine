{{ config(
    materialized='incremental',
    unique_key=['game_id', 'sport_type'],
    schema='mart_analytics',
    cluster_by=['partition_year', 'partition_month', 'partition_day'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

with matchup_metrics as (
    select
        game_id,
        'NBA' as sport_type,
        home_name,
        away_name,
        home_record,
        away_record,
        home_wins,
        home_losses,
        away_wins,
        away_losses,
        -- Win Percentages
        home_wins::float / nullif(home_wins + home_losses, 0) as home_win_pct,
        away_wins::float / nullif(away_wins + away_losses, 0) as away_win_pct,
        abs(home_wins::float / nullif(home_wins + home_losses, 0) -
            away_wins::float / nullif(away_wins + away_losses, 0)) as record_differential,
        record_favorite,
        winner,
        -- Matchup Analysis
        case when record_favorite = winner then true else false end as favorite_won,
        case
            when abs(home_wins - away_wins) <= 2 then 'Even Matchup'
            when abs(home_wins - away_wins) <= 5 then 'Slight Advantage'
            when abs(home_wins - away_wins) <= 10 then 'Clear Advantage'
            else 'Strong Advantage'
        end as matchup_type,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_nba_record_matchup') }}

    union all

    select
        game_id,
        'NFL' as sport_type,
        home_name,
        away_name,
        home_record,
        away_record,
        home_wins,
        home_losses,
        away_wins,
        away_losses,
        home_wins::float / nullif(home_wins + home_losses, 0) as home_win_pct,
        away_wins::float / nullif(away_wins + away_losses, 0) as away_win_pct,
        abs(home_wins::float / nullif(home_wins + home_losses, 0) -
            away_wins::float / nullif(away_wins + away_losses, 0)) as record_differential,
        record_favorite,
        winner,
        case when record_favorite = winner then true else false end as favorite_won,
        case
            when abs(home_wins - away_wins) = 0 then 'Even Matchup'
            when abs(home_wins - away_wins) <= 2 then 'Slight Advantage'
            when abs(home_wins - away_wins) <= 4 then 'Clear Advantage'
            else 'Strong Advantage'
        end as matchup_type,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_nfl_record_matchup') }}

    union all

    select
        game_id,
        'NHL' as sport_type,
        home_name,
        away_name,
        home_record,
        away_record,
        home_wins,
        home_losses,
        away_wins,
        away_losses,
        home_wins::float / nullif(home_wins + home_losses, 0) as home_win_pct,
        away_wins::float / nullif(away_wins + away_losses, 0) as away_win_pct,
        abs(home_wins::float / nullif(home_wins + home_losses, 0) -
            away_wins::float / nullif(away_wins + away_losses, 0)) as record_differential,
        record_favorite,
        winner,
        case when record_favorite = winner then true else false end as favorite_won,
        case
            when abs(home_wins - away_wins) <= 3 then 'Even Matchup'
            when abs(home_wins - away_wins) <= 7 then 'Slight Advantage'
            when abs(home_wins - away_wins) <= 12 then 'Clear Advantage'
            else 'Strong Advantage'
        end as matchup_type,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_nhl_record_matchup') }}

    union all

    select
        game_id,
        'CFB' as sport_type,
        home_name,
        away_name,
        home_record,
        away_record,
        home_wins,
        home_losses,
        away_wins,
        away_losses,
        home_wins::float / nullif(home_wins + home_losses, 0) as home_win_pct,
        away_wins::float / nullif(away_wins + away_losses, 0) as away_win_pct,
        abs(home_wins::float / nullif(home_wins + home_losses, 0) -
            away_wins::float / nullif(away_wins + away_losses, 0)) as record_differential,
        record_favorite,
        winner,
        case when record_favorite = winner then true else false end as favorite_won,
        case
            when abs(home_wins - away_wins) <= 1 then 'Even Matchup'
            when abs(home_wins - away_wins) <= 3 then 'Slight Advantage'
            when abs(home_wins - away_wins) <= 5 then 'Clear Advantage'
            else 'Strong Advantage'
        end as matchup_type,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_cfb_record_matchup') }}
)

select * from matchup_metrics
{% if is_incremental() %}
where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
{% endif %}
