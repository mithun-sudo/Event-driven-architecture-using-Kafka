with bowlers_with_maiden as (
select 
bowler_name,
sum(is_maiden) as maidens
from
(select 
bowler_name,
over,
case when max(is_run) = 1 then 0 else 1 end as is_maiden
from (
select bowler_name, 
split_part(ball_name, '.', 1) as over, 
case when (actual_runs + extras) > 0 then 1 else 0 end as is_run,
ball_name,
actual_runs + extras as total_runs
from ball_by_ball
where innings_no = '2' and match_id = '1428') A
group by bowler_name, over) B
group by bowler_name),
bowling_summary as (
select 
bowler_name,
sum(actual_runs) + sum(extras) as runs_conceded,
case when 
MOD((count(*) - sum(case when extras > 0 then 1 else 0 end)),6) > 0
then
concat(cast(((count(*) - sum(case when extras > 0 then 1 else 0 end)) / 6) as varchar) ,'.',
cast((MOD((count(*) - sum(case when extras > 0 then 1 else 0 end)),6)) as varchar)) else
cast(((count(*) - sum(case when extras > 0 then 1 else 0 end)) / 6) as varchar)
end 
as total_balls_bowled,
sum(case when is_wicket = '1' then 1 else 0 end) as wickets,
round((sum(actual_runs) + sum(extras)) * 6.0 / (count(*) - sum(case when extras > 0 then 1 else 0 end)), 2) as economy,
min(cast(ball_name as float)) as min_ball_name
from ball_by_ball 
where innings_no = '2' and match_id = '1428'
group by bowler_name)
select 
bowling_summary.bowler_name,
runs_conceded, 
total_balls_bowled,
wickets,
economy,
bowlers_with_maiden.maidens
from bowling_summary
left join bowlers_with_maiden on bowling_summary.bowler_name = bowlers_with_maiden.bowler_name
order by min_ball_name;