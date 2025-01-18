{% test season_record_limit(model, column_name, wins_column, losses_column, max_games) %}

select *
from {{ model }}
where {{ wins_column }} + {{ losses_column }} > {{ max_games }}
  and {{ wins_column }} is not null
  and {{ losses_column }} is not null

{% endtest %}


{% test win_pct_range(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < 0 or {{ column_name }} > 1)

{% endtest %}


{% test record_format(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} is not null
  and not (
    regexp_like({{ column_name }}, '^[0-9]+-[0-9]+-[0-9]+$')
    or regexp_like({{ column_name }}, '^[0-9]+-[0-9]+$')
  )

{% endtest %}
