/*
Cumulative table generation query: Write a query that populates the actors table one year at a time.
*/


WITH prev AS (
    SELECT *
    FROM actors
	where year = 1975
),

this_year_films AS (
    SELECT
        actorid,
        ARRAY_AGG(
            ROW(film, votes, rating, filmid)::film_struct
            ORDER BY year
        ) AS films,
	CASE 
            WHEN AVG(rating) > 8 THEN 'star'
            WHEN AVG(rating) > 7 THEN 'good'
            WHEN AVG(rating) > 6 THEN 'average'
            ELSE 'bad'
    END::quality_class AS quality_class,
	year as "year"
    FROM actor_films
	where year = 1976
    GROUP BY actorid, year
),

merged AS (
    SELECT
        COALESCE(p.actorid, n.actorid) AS actorid,
        COALESCE(p.films, ARRAY[]::film_struct[]) || COALESCE(n.films, ARRAY[]::film_struct[]) AS films,
        COALESCE(n.quality_class, p.quality_class) AS quality_class,
		COALESCE(n.year,p.year+1),
		n.actorid IS NOT NULL AS is_active

    FROM prev p
    FULL OUTER JOIN this_year_films n ON p.actorid = n.actorid
)

INSERT INTO actors
SELECT *
FROM merged;