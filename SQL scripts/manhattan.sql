select 
sum(actual_runs) + sum(extras) as runs,
sum(cast(is_wicket as integer)) as wickets,
split_part(ball_name, '.', 1) as over,
array_remove(array_agg(case when is_wicket = '0' then NULL else batsman_name end), NULL) as wicket_of_batsman
from ball_by_ball
where innings_no = '2' and match_id = '1428' 
group by split_part(ball_name, '.', 1) 
order by cast(split_part(ball_name, '.', 1) as integer);