{% test nhl_metrics_validation(model, column_name) %}

select *
from {{ model }}
where overtime_wins > total_overtime_games
   or total_wins > total_games
   or avg_assists_per_goal < 0
   or overtime_wins is null
   or total_wins is null
   or avg_assists_per_goal is null

{% endtest %}
