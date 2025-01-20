{% test periods_match_linescores(model, column_name='total_periods') %}

with validation as (
    select
        game_id,
        {{ column_name }} as periods,
        -- Get actual period counts from quarter scores
        (case when home_q1_score is not null then 1 else 0 end +
         case when home_q2_score is not null then 1 else 0 end +
         case when home_q3_score is not null then 1 else 0 end +
         case when home_q4_score is not null then 1 else 0 end +
         case when home_ot_scores is not null then array_size(home_ot_scores) else 0 end
        ) as home_periods,
        (case when away_q1_score is not null then 1 else 0 end +
         case when away_q2_score is not null then 1 else 0 end +
         case when away_q3_score is not null then 1 else 0 end +
         case when away_q4_score is not null then 1 else 0 end +
         case when away_ot_scores is not null then array_size(away_ot_scores) else 0 end
        ) as away_periods
    from {{ model }}
)
select *
from validation
where periods != home_periods
    or periods != away_periods
    or home_periods != away_periods

{% endtest %}