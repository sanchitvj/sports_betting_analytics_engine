{% test nba_trends_metrics(model, column_name, min_assists=15, min_rebounds=30) %}

select *
from {{ model }}
where avg_assists < {{ min_assists }}
   or avg_rebounds < {{ min_rebounds }}
   or total_wins > total_games

{% endtest %}

{% test array_content_validation(model, column_name) %}

select *
from {{ model }}
where array_size({{ column_name }}) < 1
and (
    get({{ column_name }}, 0) is null
    or try_cast(get({{ column_name }}, 0):game_id::string as integer) is null
    or get({{ column_name }}, 0):team_type not in ('HOME', 'AWAY')
    or try_cast(get({{ column_name }}, 0):points_scored::string as float) is null
)

{% endtest %}

