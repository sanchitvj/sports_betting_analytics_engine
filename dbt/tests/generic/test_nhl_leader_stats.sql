{% test nhl_performance_validation(model, column_name) %}

select *
from {{ model }}
where (
    -- Stat value validations
    (leader_type = 'GOALS' and stat_value > 5)
    or (leader_type = 'ASSISTS' and stat_value > 5)
    or (leader_type = 'POINTS' and stat_value > 8)
    -- Performance milestone validation
    or performance_milestone not in ('Hat Trick', 'Brace', 'Four Point Night', 'Regular Performance')
    -- Hat trick validation
    or total_hat_tricks > games_as_leader
    -- Null checks
    or stat_value is null
    or performance_milestone is null
    or total_hat_tricks is null
    or games_as_leader is null
)

{% endtest %}
