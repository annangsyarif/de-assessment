drop table monthly_summary_salary_per_hour if exist;
create table monthly_summary_salary_per_hour as 
-- total hour per branch per day
with total_hour_per_branch_by_day as (
	-- cleaning checkin and checkout (assume there is a night shift with different checkin/checkout days)
	with cleaned_ts as (
		select "date", employee_id, e.branch_id, "date" + checkin checkin,
			case
				when checkout < checkin then "date" + 1 + checkout
				else "date" + checkout
			end as checkout
		from timesheets t
		left join employees e on e.employe_id = t.employee_id
		-- filter for non-resigned employee
		where e.resign_date is null
	)
	select extract(year from "date") "year", extract(month from "date") "month", branch_id, sum(extract(hour from checkout - checkin)) total_hour from cleaned_ts
	group by "date", branch_id
),
-- total hour per branch per month
total_hour_per_branch_by_month  as (
	select "year", "month", branch_id, sum(total_hour) total_hour from total_hour_per_branch_by_day
	group by "year", "month", branch_id	
	order by "year" desc, "month" desc, branch_id
),
-- total salary per branch with non-resigner employee
total_salary_per_branch as (
	select branch_id, sum(salary) total_salary from employees
	where resign_date is null
	group by branch_id
)
-- summary of all
select thpb."year", thpb."month", thpb.branch_id, round(tspb.total_salary/thpb.total_hour,2) salary_per_hour
from total_hour_per_branch_by_month thpb
join total_salary_per_branch tspb on tspb.branch_id = thpb.branch_id;