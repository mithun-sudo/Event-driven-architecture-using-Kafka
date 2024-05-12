select 
batsman_name, 
sum(actual_runs) as runs,
sum(ball) as balls,
sum(case when is_four = '1' then 1 else 0 end) as fours,
sum(case when is_six = '1' then 1 else 0 end) as sixes,
round(sum(actual_runs) * 100.0/sum(ball), 2) as strike_rate,
case when max(is_wicket) = '1' then 'OUT' else 'NOT OUT' end as is_out,
max(case when is_wicket = '0' then NULL else bowler_name end) as bowler_name
from
(select 
case when coalesce(lead(ball_name) over (order by cast(ball_name as float)) != ball_name, true)  then 1 else 0 end as ball, 
* 
from ball_by_ball 
where innings_no = '1' and match_id = '1428') A
group by batsman_name
order by min(ball_name)