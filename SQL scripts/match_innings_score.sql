create view match_innings_score as
select team_name, sum(actual_runs), 'First innings' as innings from ball_by_ball 
where innings_no = '1' and match_id = '1428'
group by team_name
union
select team_name, sum(actual_runs), 'Second innings' as innings from ball_by_ball 
where innings_no = '2' and match_id = '1428'
group by team_name
order by innings;