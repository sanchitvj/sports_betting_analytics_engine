{% macro get_overtime_scores(linescores_array, periods) %}
case
    when array_size({{ linescores_array }}) > {{ periods }}
    then array_slice({{ linescores_array }}, {{ periods }}, array_size({{ linescores_array }}))
    else null
end
{% endmacro %}