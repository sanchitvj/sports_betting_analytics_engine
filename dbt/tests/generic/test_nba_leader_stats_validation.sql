{% test leader_stats_validation(model, column_name) %}

with stat_limits as (
    select *
    from {{ model }}
    where (leader_type = 'POINTS' and stat_value > 100)
       or (leader_type = 'REBOUNDS' and stat_value > 40)
       or (leader_type = 'ASSISTS' and stat_value > 30)
       or (leader_type = 'RATING' and stat_value > 120)
       or avg_stat_value > season_high
       or exceptional_performance not in (0, 1)
       or stat_value is null
       or avg_stat_value is null
       or season_high is null
       or exceptional_performance is null
)

select
    *,
    case
        when leader_type = 'POINTS' and stat_value > 100 then 'Points exceed maximum (100)'
        when leader_type = 'REBOUNDS' and stat_value > 40 then 'Rebounds exceed maximum (40)'
        when leader_type = 'ASSISTS' and stat_value > 30 then 'Assists exceed maximum (30)'
        when leader_type = 'RATING' and stat_value > 120 then 'Rating exceeds maximum (50)'
        when avg_stat_value > season_high then 'Average exceeds season high'
        when exceptional_performance not in (0, 1) then 'Invalid exceptional performance flag'
        when stat_value is null then 'Null stat value'
        when avg_stat_value is null then 'Null average stat value'
        when season_high is null then 'Null season high'
        when exceptional_performance is null then 'Null exceptional performance'
    end as failure_reason
from stat_limits

{% endtest %}
