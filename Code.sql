--Daily Activity Table

--viewing the data of dailyactivity


select * from GoogleCapstone_Part2..dailyActivity_merged

--Id,total_steps,total_distance, calories_burnt and percentage of calori burnt per step
select id, activity_date, total_steps, total_distance,calories, cast(calories as float)/cast(total_steps as float) as PercentCalorie_PerStep 
from GoogleCapstone_Part2..dailyActivity_merged
where total_steps != 0 

--total calories burnt by users day-wise
select Id, activity_date, sum(cast(calories as int))  OVER (PARTITION BY Id order by Id,activity_date) as totalcalories_burnt
from GoogleCapstone_Part2..dailyActivity_merged
where id is not null
order by 1,2

--count the number of user 
select count(distinct Id) as TotalId 
from GoogleCapstone_Part2..dailyActivity_merged

--sum of total days the user logged in
select Id,activity_date,count(cast(id as float)) over (partition by id order by id,activity_date) as TotalDays 
from googlecapstone_part2..dailyActivity_merged
order by 1,2

--Active time spent in minutes by user per day
select Id, activity_date,convert(float, cast(very_active_minutes as float) + cast(fairly_active_minutes as float)+ 
cast(light_active_minutes as float)) as total_minutes
from GoogleCapstone_Part2..dailyActivity_merged
order by 1,2

--finding total time spent by a user using temp table
drop table if exists #total_minutes
SELECT *
INTO #total_minutes
FROM 
(select Id,convert(float, cast(very_active_minutes as float) + cast(fairly_active_minutes as float)+ 
cast(light_active_minutes as float)) as total_minutes
from GoogleCapstone_Part2..dailyActivity_merged
) as total_min

select distinct Id,sum(total_minutes) over (partition by id order by id) as total_timespent
from #total_minutes
group by id,total_minutes

--mean,max,average time spend by a user in minutes
select min(total_minutes) as min_timespent , max(total_minutes) as max_timespent, avg(total_minutes) as avg_timespent
from #total_minutes




--Heart Rate Table



--Viewing average heart rate per seconds

select * from GoogleCapstone_Part2..heartrate_seconds_merged


--total number of users in hear rate customer record
select count(distinct id)from googlecapstone_part2..heartrate_seconds_merged


--total,average heart beat in second recorded by id
drop table if exists #heartbeat
select *
into #heartbeat
from (
select id,activity_date_time_seconds,sum(convert(float,value)) over(partition by activity_date_time_seconds order by activity_date_time_seconds) as totabeats_minutes_recorded,
avg(convert(float,value)) over(partition by activity_date_time_seconds order by id) as avgbeats_second_recorded
from GoogleCapstone_Part2..heartrate_seconds_merged
) as avg_beat
order by 1

select distinct * from #heartbeat


--Hourly Intensity Table



--Viewing hourly_intensity table
select * from GoogleCapstone_Part2..hourly_intensity
--viewing total_intensity vs calories
select id,activity_date_time, total_steps,total_intensity,cast(calories as float)/cast(total_intensity as float) as avgcaloriesburnt_intensityHour
from GoogleCapstone_Part2..hourly_intensity
where total_intensity !=0

--total number of users in hourly_intensity record
select count(distinct id)from googlecapstone_part2..hourly_intensity


--finding activity date,total steps,total intensity,average intensity,totalcalories burnt in a day using temp table inside temp table for a user
--first temp table

drop table if exists #totalof_day
SELECT *
INTO #totalof_day
FROM (
select * , cast(activity_date_time as date) as activity_date
from GoogleCapstone_Part2..hourly_intensity 
    ) as tempday

--second temp table
drop table if exists #day
SELECT *
INTO #day
FROM (			
select Id,activity_date,sum(convert(float ,total_steps)) over (partition by activity_date order by activity_date,id) as totalsteps_day,
sum(convert(float , total_intensity)) over (partition by activity_date order by activity_date,id) as totalintensity_day,
sum(cast(average_intensity as float)) over (partition by activity_date order by activity_date,id) as averageintensity_day,
avg(cast(calories as float)) over(partition by activity_date order by activity_date,id) as avgcalories_day
from #totalof_day) as dayday

select distinct * from #day



--Viewing BMI Table



select * from GoogleCapstone_Part2..weightLogInfo_merged

--viewing id,Bmi,activity_date_time, weight_kg,BMI and eveluating bmi report
select id,activity_date_time,weight_kg, Bmi,
case when BMI<18.5 then 'underweight' 
when BMI <24.9 then 'normal' 
when BMI<30 then 'overweight' 
when bmi > 30 then 'obese'
end as Bmi_report
from GoogleCapstone_Part2..weightLogInfo_merged


--Weight Log Info Table


--Difference in weight every day of a user
--  negative number indicates the weight loss and positive number indicates the weight gain
select id,weight_kg,
       (weight_kg-
        lag(weight_kg,1,weight_kg)over (partition by id order by id)
       ) as weight_diff_kg
from GoogleCapstone_Part2..weightLogInfo_merged


--Sleep Day Table


--viewing sleepday
select * , cast(total_minutes_asleep as int)/60 as sleep_hours from GoogleCapstone_Part2..sleepDay_merged