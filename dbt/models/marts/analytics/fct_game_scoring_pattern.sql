{{ config(
    materialized='incremental',
    unique_key=['game_id', 'team_name', 'sport_type'],
    schema='analytics',
    incremental_strategy='merge',
    cluster_by=['partition_year', 'partition_month', 'partition_day']
) }}

with game_scoring as (
    select
        game_id,
        'NBA' as sport_type,
        start_time,
        'HOME' as team_type,
        home_name as team_name,
        -- Quarter Scoring
        home_q1_score as q1_score,
        home_q2_score as q2_score,
        home_q3_score as q3_score,
        home_q4_score as q4_score,
        home_ot_scores as ot_scores,
        -- Scoring Analysis
        home_q1_score + home_q2_score as first_half_score,
        home_q3_score + home_q4_score as second_half_score,
        home_score as total_score,
        -- Scoring Patterns
        home_q1_score / nullif(home_score, 0) * 100 as q1_scoring_pct,
        home_q2_score / nullif(home_score, 0) * 100 as q2_scoring_pct,
        home_q3_score / nullif(home_score, 0) * 100 as q3_scoring_pct,
        home_q4_score / nullif(home_score, 0) * 100 as q4_scoring_pct,
        -- Game Context
        case when home_score > away_score then true else false end as is_winner,
        abs(home_score - away_score) as score_margin,
        case
            when abs(home_score - away_score) <= 5 then 'Very Close'
            when abs(home_score - away_score) <= 10 then 'Close'
            when abs(home_score - away_score) <= 20 then 'Moderate'
            else 'Blowout'
        end as game_competitiveness,
        -- Quarter Performance
        case
            when home_q1_score > home_q2_score
            and home_q1_score > home_q3_score
            and home_q1_score > home_q4_score then 'Q1'
            when home_q2_score > home_q1_score
            and home_q2_score > home_q3_score
            and home_q2_score > home_q4_score then 'Q2'
            when home_q3_score > home_q1_score
            and home_q3_score > home_q2_score
            and home_q3_score > home_q4_score then 'Q3'
            else 'Q4'
        end as highest_scoring_quarter,
        number_of_overtimes,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_nba_game_stats') }}

    union all

    select
        game_id,
        'NBA' as sport_type,
        start_time,
        'AWAY' as team_type,
        away_name as team_name,
        away_q1_score as q1_score,
        away_q2_score as q2_score,
        away_q3_score as q3_score,
        away_q4_score as q4_score,
        away_ot_scores as ot_scores,
        away_q1_score + away_q2_score as first_half_score,
        away_q3_score + away_q4_score as second_half_score,
        away_score as total_score,
        away_q1_score / nullif(away_score, 0) * 100 as q1_scoring_pct,
        away_q2_score / nullif(away_score, 0) * 100 as q2_scoring_pct,
        away_q3_score / nullif(away_score, 0) * 100 as q3_scoring_pct,
        away_q4_score / nullif(away_score, 0) * 100 as q4_scoring_pct,
        case when away_score > home_score then true else false end as is_winner,
        abs(home_score - away_score) as score_margin,
        case
            when abs(home_score - away_score) <= 5 then 'Very Close'
            when abs(home_score - away_score) <= 10 then 'Close'
            when abs(home_score - away_score) <= 20 then 'Moderate'
            else 'Blowout'
        end as game_competitiveness,
        case
            when away_q1_score > away_q2_score
            and away_q1_score > away_q3_score
            and away_q1_score > away_q4_score then 'Q1'
            when away_q2_score > away_q1_score
            and away_q2_score > away_q3_score
            and away_q2_score > away_q4_score then 'Q2'
            when away_q3_score > away_q1_score
            and away_q3_score > away_q2_score
            and away_q3_score > away_q4_score then 'Q3'
            else 'Q4'
        end as highest_scoring_quarter,
        number_of_overtimes,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_nba_game_stats') }}

    union all

    select
        game_id,
        'NFL' as sport_type,
        start_time,
        'HOME' as team_type,
        home_name as team_name,
        home_q1_score as q1_score,
        home_q2_score as q2_score,
        home_q3_score as q3_score,
        home_q4_score as q4_score,
        home_ot_scores as ot_scores,
        home_q1_score + home_q2_score as first_half_score,
        home_q3_score + home_q4_score as second_half_score,
        home_score as total_score,
        home_q1_score / nullif(home_score, 0) * 100 as q1_scoring_pct,
        home_q2_score / nullif(home_score, 0) * 100 as q2_scoring_pct,
        home_q3_score / nullif(home_score, 0) * 100 as q3_scoring_pct,
        home_q4_score / nullif(home_score, 0) * 100 as q4_scoring_pct,
        case when home_score > away_score then true else false end as is_winner,
        abs(home_score - away_score) as score_margin,
        case
            when abs(home_score - away_score) <= 3 then 'One Possession'
            when abs(home_score - away_score) <= 7 then 'One Score'
            when abs(home_score - away_score) <= 14 then 'Two Scores'
            when abs(home_score - away_score) <= 21 then 'Three Scores'
            else 'Blowout'
        end as game_competitiveness,
        case
            when home_q1_score > home_q2_score
            and home_q1_score > home_q3_score
            and home_q1_score > home_q4_score then 'Q1'
            when home_q2_score > home_q1_score
            and home_q2_score > home_q3_score
            and home_q2_score > home_q4_score then 'Q2'
            when home_q3_score > home_q1_score
            and home_q3_score > home_q2_score
            and home_q3_score > home_q4_score then 'Q3'
            else 'Q4'
        end as highest_scoring_quarter,
        number_of_overtimes,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_nfl_game_stats') }}

    union all

    select
        game_id,
        'NFL' as sport_type,
        start_time,
        'AWAY' as team_type,
        away_name as team_name,
        away_q1_score as q1_score,
        away_q2_score as q2_score,
        away_q3_score as q3_score,
        away_q4_score as q4_score,
        away_ot_scores as ot_scores,
        away_q1_score + away_q2_score as first_half_score,
        away_q3_score + away_q4_score as second_half_score,
        away_score as total_score,
        away_q1_score / nullif(away_score, 0) * 100 as q1_scoring_pct,
        away_q2_score / nullif(away_score, 0) * 100 as q2_scoring_pct,
        away_q3_score / nullif(away_score, 0) * 100 as q3_scoring_pct,
        away_q4_score / nullif(away_score, 0) * 100 as q4_scoring_pct,
        case when away_score > home_score then true else false end as is_winner,
        abs(home_score - away_score) as score_margin,
        case
            when abs(home_score - away_score) <= 3 then 'One Possession'
            when abs(home_score - away_score) <= 7 then 'One Score'
            when abs(home_score - away_score) <= 14 then 'Two Scores'
            when abs(home_score - away_score) <= 21 then 'Three Scores'
            else 'Blowout'
        end as game_competitiveness,
        case
            when away_q1_score > away_q2_score
            and away_q1_score > away_q3_score
            and away_q1_score > away_q4_score then 'Q1'
            when away_q2_score > away_q1_score
            and away_q2_score > away_q3_score
            and away_q2_score > away_q4_score then 'Q2'
            when away_q3_score > away_q1_score
            and away_q3_score > away_q2_score
            and away_q3_score > away_q4_score then 'Q3'
            else 'Q4'
        end as highest_scoring_quarter,
        number_of_overtimes,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_nfl_game_stats') }}

    union all

    select
        game_id,
        'CFB' as sport_type,
        start_time,
        'HOME' as team_type,
        home_name as team_name,
        home_q1_score as q1_score,
        home_q2_score as q2_score,
        home_q3_score as q3_score,
        home_q4_score as q4_score,
        home_ot_scores as ot_scores,
        home_q1_score + home_q2_score as first_half_score,
        home_q3_score + home_q4_score as second_half_score,
        home_score as total_score,
        home_q1_score / nullif(home_score, 0) * 100 as q1_scoring_pct,
        home_q2_score / nullif(home_score, 0) * 100 as q2_scoring_pct,
        home_q3_score / nullif(home_score, 0) * 100 as q3_scoring_pct,
        home_q4_score / nullif(home_score, 0) * 100 as q4_scoring_pct,
        case when home_score > away_score then true else false end as is_winner,
        abs(home_score - away_score) as score_margin,
        case
            when abs(home_score - away_score) <= 3 then 'One Possession'
            when abs(home_score - away_score) <= 7 then 'One Score'
            when abs(home_score - away_score) <= 14 then 'Two Scores'
            when abs(home_score - away_score) <= 21 then 'Three Scores'
            else 'Blowout'
        end as game_competitiveness,
        case
            when home_q1_score > home_q2_score
            and home_q1_score > home_q3_score
            and home_q1_score > home_q4_score then 'Q1'
            when home_q2_score > home_q1_score
            and home_q2_score > home_q3_score
            and home_q2_score > home_q4_score then 'Q2'
            when home_q3_score > home_q1_score
            and home_q3_score > home_q2_score
            and home_q3_score > home_q4_score then 'Q3'
            else 'Q4'
        end as highest_scoring_quarter,
        number_of_overtimes,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_cfb_game_stats') }}

    union all

    select
        game_id,
        'CFB' as sport_type,
        start_time,
        'AWAY' as team_type,
        away_name as team_name,
        away_q1_score as q1_score,
        away_q2_score as q2_score,
        away_q3_score as q3_score,
        away_q4_score as q4_score,
        away_ot_scores as ot_scores,
        away_q1_score + away_q2_score as first_half_score,
        away_q3_score + away_q4_score as second_half_score,
        away_score as total_score,
        away_q1_score / nullif(away_score, 0) * 100 as q1_scoring_pct,
        away_q2_score / nullif(away_score, 0) * 100 as q2_scoring_pct,
        away_q3_score / nullif(away_score, 0) * 100 as q3_scoring_pct,
        away_q4_score / nullif(away_score, 0) * 100 as q4_scoring_pct,
        case when away_score > home_score then true else false end as is_winner,
        abs(home_score - away_score) as score_margin,
        case
            when abs(home_score - away_score) <= 3 then 'One Possession'
            when abs(home_score - away_score) <= 7 then 'One Score'
            when abs(home_score - away_score) <= 14 then 'Two Scores'
            when abs(home_score - away_score) <= 21 then 'Three Scores'
            else 'Blowout'
        end as game_competitiveness,
        case
            when away_q1_score > away_q2_score
            and away_q1_score > away_q3_score
            and away_q1_score > away_q4_score then 'Q1'
            when away_q2_score > away_q1_score
            and away_q2_score > away_q3_score
            and away_q2_score > away_q4_score then 'Q2'
            when away_q3_score > away_q1_score
            and away_q3_score > away_q2_score
            and away_q3_score > away_q4_score then 'Q3'
            else 'Q4'
        end as highest_scoring_quarter,
        number_of_overtimes,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_cfb_game_stats') }}

    union all

    select
        game_id,
        'NHL' as sport_type,
        start_time,
        'HOME' as team_type,
        home_name as team_name,
        home_q1_score as q1_score,
        home_q2_score as q2_score,
        home_q3_score as q3_score,
        null as q4_score,
        home_ot_scores as ot_scores,
        home_q1_score + (home_q2_score/2)  as first_half_score,
        (home_q2_score/2) + home_q3_score as second_half_score,
        home_score as total_score,
        home_q1_score / nullif(home_score, 0) * 100 as q1_scoring_pct,
        home_q2_score / nullif(home_score, 0) * 100 as q2_scoring_pct,
        home_q3_score / nullif(home_score, 0) * 100 as q3_scoring_pct,
        null q4_scoring_pct,
        case when home_score > away_score then true else false end as is_winner,
        abs(home_score - away_score) as score_margin,
        case
            when abs(home_score - away_score) = 1 then 'One Goal'
            when abs(home_score - away_score) = 2 then 'Two Goals'
            when abs(home_score - away_score) >= 3 then 'Three+ Goals'
            else 'Tie'
        end as game_competitiveness,
        case
            when away_q1_score > away_q2_score and away_q1_score > away_q3_score then 'Q1'
            when away_q2_score > away_q1_score and away_q2_score > away_q3_score then 'Q2'
            else 'Q3'
        end as highest_scoring_quarter,
        number_of_overtimes,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_nhl_game_stats') }}

    union all

    select
        game_id,
        'NHL' as sport_type,
        start_time,
        'AWAY' as team_type,
        away_name as team_name,
        away_q1_score as q1_score,
        away_q2_score as q2_score,
        away_q3_score as q3_score,
        null as q4_score,
        away_ot_scores as ot_scores,
        away_q1_score + (away_q2_score/2) as first_half_score,
        (away_q2_score/2) + away_q3_score as second_half_score,
        away_score as total_score,
        away_q1_score / nullif(away_score, 0) * 100 as q1_scoring_pct,
        away_q2_score / nullif(away_score, 0) * 100 as q2_scoring_pct,
        away_q3_score / nullif(away_score, 0) * 100 as q3_scoring_pct,
        null as q4_scoring_pct,
        case when away_score > home_score then true else false end as is_winner,
        abs(home_score - away_score) as score_margin,
        case
            when abs(home_score - away_score) = 1 then 'One Goal'
            when abs(home_score - away_score) = 2 then 'Two Goals'
            when abs(home_score - away_score) >= 3 then 'Three+ Goals'
            else 'Tie'
        end as game_competitiveness,
        case
            when away_q1_score > away_q2_score and away_q1_score > away_q3_score then 'Q1'
            when away_q2_score > away_q1_score and away_q2_score > away_q3_score then 'Q2'
            else 'Q3'
        end as highest_scoring_period,
        number_of_overtimes,
        partition_year,
        partition_month,
        partition_day,
        ingestion_timestamp
    from {{ ref('int_nhl_game_stats') }}
)

select * from game_scoring
{% if is_incremental() %}
where ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
{% endif %}
