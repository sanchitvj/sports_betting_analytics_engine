with source as (
    select * from {{ source('sports_db', 'nba_games') }}
),
staged as (
    select
        game_id,
        sport_key,
        start_time,
        home_team,
        away_team,
        venue,
        partition_year,
        partition_month,
        partition_day
    from source
)
select * from staged