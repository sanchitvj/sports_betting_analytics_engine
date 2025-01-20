{% test market_metrics_validation(model, column_name) %}

select *
from {{ model }}
where (
    -- Efficiency validation
    avg_vig < min_vig
    or avg_vig > max_vig
    or favorite_changes > price_updates
    -- Probability spread validation
    or avg_probability_spread < 0
    or avg_probability_spread > 1
    or max_probability_spread < 0
    or max_probability_spread > 1
    -- Null checks
    or avg_vig is null
    or min_vig is null
    or max_vig is null
    or favorite_changes is null
    or price_updates is null
    or avg_probability_spread is null
    or max_probability_spread is null
)

{% endtest %}
