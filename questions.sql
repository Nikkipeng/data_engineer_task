--  1. What is the most common first name among actresses?
select first_name, count(actor_id) num from actor
where gender = 'female'
group by first_name
order by num desc limit 1;
-- Michael for all 263
-- Michael for male 120
-- Michael for female 77

--  2. What is the most common first name among actors?
select first_name, count(actor_id) num from actor
where gender = 'male'
group by first_name
order by num desc;

--  3. Which Movie had the longest timespan from release to appearing on Netflix?
select show_id, title,  date_added, release_year, year(date_added) - release_year year_diff
from netflix_db.show
where release_year < year(date_added)
order by year_diff desc limit 1;

-- 93 years, no same year diff, so it's exact result

-- Which Month of the year had the most new releases historically?
--  4. Which exact month for exact year that had the most new releases historically?
select count(id) num, month(date_added) mon, year(date_added) yea from netflix_db.show
group by mon, yea
order by num desc limit 1;
-- Nov, 2019  272

--  5. Which month among 12 months during a year that had the most new releases historically?
select count(id) num, month(date_added) mon from netflix_db.show
group by mon
order by num desc limit 1;
-- Dec   833 

--  6. Which year had the largest increase year on year (percentage wise) for TV Shows?
with temp as (select count(id) num, year(date_added) yea
from netflix_db.show
where type = 'TV Show'
group by yea)

select temp1.num, temp1.yea, temp2.yea, temp2.num,
(temp2.num - temp1.num) / temp1.num * 100 as increase_yoy
from temp temp1
join temp temp2 on temp1.yea+1 = temp2.yea
order by increase_yoy desc limit 1
;
-- 2016 over 2015 increase 516.7% 

--  7. List the actresses that have appeared in a movie with Woody Harrelson more than once.
--select * from actor where name = 'Woody Harrelson';  -- actor id 30326
select distinct(show_actor1.actor_id), actor.name from show_actor show_actor1
join show_actor show_actor2 
on show_actor1.show_id = show_actor2.show_id and show_actor2.actor_id = 30326 and show_actor1.actor_id != 30326 
join actor on show_actor1.actor_id = actor.actor_id and actor.gender = 'female'
join netflix_db.show on show_actor1.show_id = show.id and show.type = 'Movie';
-- 16 actresses
-- 'Bill Murray' 'Casey Affleck' 'Clifton Collins Jr.' 'Derek Graf' 'Donald Glover' 'January Jones' 'John Carroll Lynch' 
-- 'Jonathan Loughran' 'José Zúñiga' 'Kim Dickens' 'Marisa Tomei' 'Norman Reedus' 'Paul Bettany' 'Randy Quaid'
-- 'Richard Tyson' 'Robinne Lee' 

