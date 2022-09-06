SELECT * FROM netflix_db.show
where rating is null;
-- 7 records don't have rating 

SELECT * FROM netflix_db.show
where release_year is null;
-- all have release_year 

SELECT * FROM netflix_db.show
where duration is null;
-- all have duration 

SELECT * FROM netflix_db.show
where date_added is null;
-- 10 records don't have date_added 

SELECT * FROM netflix_db.show
where type is null;
-- all have type 

SELECT * FROM netflix_db.show
where description is null;
-- all have description 


SELECT max(duration) FROM netflix_db.show
-- where type = 'TV Show'
;
-- max 16 season
--  max 312 mins

SELECT type, count(id) FROM netflix_db.show
group by type;
-- 2410 TV Show 
-- 5377 Movie 

SELECT rating, count(id) FROM netflix_db.show
-- where type = 'TV Show'
-- where type = 'Movie'
group by rating;
-- TV-MA is the most, and then TV-14 

select * from netflix_db.show left join show_actor on show.id = show_actor.show_id
where show_actor.actor_id is null;
-- 719 don't have cast
 
select * from netflix_db.show left join show_director on show.id = show_director.show_id
where show_director.director_id is null;
-- 2390 don't have director

select * from netflix_db.show left join list_category on show.id = list_category.show_id
where list_category.category_id is null;
-- 56 don't have category

select * from netflix_db.show left join show_country on show.id = show_country.show_id
where show_country.country_id is null;
-- 507 don't have country

-- 31 rows date_added smaller than release year