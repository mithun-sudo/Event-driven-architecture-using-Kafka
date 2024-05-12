select 
wicket_key as wicket,
cumulative_runs,
batsman_name,
bowler_name,
ball_name
from 
(select 
wicket_key,
case when lag(wicket_key) over (order by cast(ball_name as float)) != wicket_key then 1 else null end as fall_of_wicket,
cumulative_runs,
batsman_name,
bowler_name,
ball_name
from
(select 
sum(cast(is_wicket as integer)) over (order by cast(ball_name as float)) as wicket_key,
sum(actual_runs+extras) over (order by cast(ball_name as float)) as cumulative_runs,
* 
from ball_by_ball
where innings_no = '2' and match_id = '1428') A ) B
where fall_of_wicket = 1