{% test periods_match_linescores(model, column_name='total_periods') %}

with validation as (
    select
        game_id,
        {{ column_name }} as periods,
        array_size(home_linescores) as home_periods,
        array_size(away_linescores) as away_periods
    from {{ model }}
),
validation_check as (
    select *
    from validation
    where periods != home_periods
    or periods != away_periods
    or home_periods != away_periods
)

select *
from validation_check

{% endtest %}
