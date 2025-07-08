/*
Instead of loading actors table year by year, the following query loads the entire
data into actors table at once
*/

WITH bounds AS (
    SELECT 
        MIN(year) AS min_year,
        MAX(year) AS max_year
    FROM actor_films
),
years AS (
    SELECT generate_series(min_year, max_year) AS year
    FROM bounds
),

actor_first_year AS(
	select actorid, min(year) as "first_year"
	from actor_films
	group by actorid
),

actors_and_years AS(
	SELECT * from actor_first_year
	join years on actor_first_year.first_year <= years.year
),

windowed as(
	select 
	aay.actorid,
	aay.year,
	ARRAY_REMOVE(
		ARRAY_AGG(
			case when af.year is not null Then ROW(af.film, af.votes, af.rating, af.filmid, af.year)::film_struct 
		END) over (partition by aay.actorid order by coalesce(aay.year, af.year)),
		NULL	
		) as films

		from actors_and_years aay 
		left join actor_films af on aay.actorid = af.actorid
		and aay.year = af.year
	)

INSERT INTO actors

select w.actorid,
		w.films,
		CASE
        WHEN AVG((films[CARDINALITY(films)]::film_struct).rating) > 8 THEN 'star'
        WHEN AVG((films[CARDINALITY(films)]::film_struct).rating) > 7 THEN 'good'
        WHEN AVG((films[CARDINALITY(films)]::film_struct).rating) > 6 THEN 'average'
        ELSE 'bad'
    END::quality_class AS quality_class,
	w.year,
	(films[cardinality(films)]::film_struct).year = w.year as is_active

from windowed w
group by 1,2,4,5
