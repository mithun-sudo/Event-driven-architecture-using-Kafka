select
*
from
(select
max(next_over) as over,
max(case when current_over != next_over then round((running_sum * 1.0 / ball_number) * 6, 2) else NULL end) as run_rate
from (select 
*, 
sum(actual_runs + extras) over (order by cast(ball_name as float)) as running_sum,
dense_rank() over (order by cast(ball_name as float)) as ball_number,
split_part(ball_name, '.', 1) as current_over,
coalesce(lead(split_part(ball_name, '.', 1)) over (order by cast(ball_name as float)), '20') as next_over
from ball_by_ball
where innings_no = '2' and match_id = '1428') A
group by current_over) B
order by cast(over as integer)