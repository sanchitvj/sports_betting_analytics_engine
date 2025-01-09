{{ config(
    materialized='table',
    unique_key='date_id',
    schema='mart_core'
) }}

-- earliest and latest dates from source games
with source_date_range as (
    select
        min(start_time::date) as min_date,
        max(start_time::date) as max_date
    from (
        select start_time from {{ ref('int_nfl_game_stats') }}
        union all
        select start_time from {{ ref('int_nba_game_stats') }}
        union all
        select start_time from {{ ref('int_nhl_game_stats') }}
        union all
        select start_time from {{ ref('int_cfb_game_stats') }}
    )
),
-- Generate date sequence
date_range as (
    select dateadd(day, seq4(), (select min_date from source_date_range)) as date_id
    from table(generator(rowcount => datediff('day',
        (select min_date from source_date_range),
        (select max_date from source_date_range)) + 365)) -- Add buffer for future dates
),
-- Get NFL weeks from actual games
nfl_weeks as (
    select distinct
        date_trunc('week', start_time)::date as week_start,
        partition_year as season_year,
        case
            when month(start_time) >= 9 then
                datediff('week',
                    date_trunc('week',
                        to_date(concat(partition_year, '-09-01'), 'YYYY-MM-DD')
                    ),
                    date_trunc('week', start_time)
                ) + 1
            when month(start_time) <= 2 then
                datediff('week',
                    date_trunc('week',
                        to_date(concat(partition_year-1, '-09-01'), 'YYYY-MM-DD')
                    ),
                    date_trunc('week', start_time)
                ) + 1
        end as week_number
    from {{ ref('int_nfl_game_stats') }}
),

cfb_weeks as (
    select distinct
        date_trunc('week', start_time)::date as week_start,
        partition_year as season_year,
        case
            when month(start_time) >= 8 then
                datediff('week',
                    date_trunc('week',
                        to_date(concat(partition_year, '-08-25'), 'YYYY-MM-DD')
                    ),
                    date_trunc('week', start_time)
                ) + 1
            when month(start_time) = 1 then
                datediff('week',
                    date_trunc('week',
                        to_date(concat(partition_year-1, '-08-25'), 'YYYY-MM-DD')
                    ),
                    date_trunc('week', start_time)
                ) + 1
        end as week_number
    from {{ ref('int_cfb_game_stats') }}
),

base_dates as (
    select
        d.date_id,
        year(d.date_id) as year,
        month(d.date_id) as month,
        day(d.date_id) as day,
        dayofweek(d.date_id) as day_of_week,
        case when dayofweek(d.date_id) in (0,6) then true else false end as is_weekend,

        -- NFL Season Info
        nw.week_number as nfl_week,
        case
            when month(d.date_id) between 9 and 12 then 'Regular Season'
            when month(d.date_id) = 1 then 'Wild Card'
            when month(d.date_id) = 2 then 'Super Bowl'
            else 'Off Season'
        end as nfl_season_type,

        -- CFB Season Info
        cw.week_number as cfb_week,
        case
            when month(d.date_id) between 8 and 12 then 'Regular Season'
            when month(d.date_id) = 1 then 'Bowl Games'
            else 'Off Season'
        end as cfb_season_type,

        -- NBA Season Info
        case
            when month(d.date_id) between 10 and 4 then 'Regular Season'
            when month(d.date_id) between 4 and 6 then 'Playoffs'
            else 'Off Season'
        end as nba_season_type,

        -- NHL Season Info
        case
            when month(d.date_id) between 10 and 4 then 'Regular Season'
            when month(d.date_id) between 4 and 6 then 'Stanley Cup Playoffs'
            else 'Off Season'
        end as nhl_season_type
    from date_range d
    left join nfl_weeks nw
        on date_trunc('week', d.date_id)::date = nw.week_start
    left join cfb_weeks cw
        on date_trunc('week', d.date_id)::date = cw.week_start
    where d.date_id <= current_date()
)
select * from base_dates
order by date_id
