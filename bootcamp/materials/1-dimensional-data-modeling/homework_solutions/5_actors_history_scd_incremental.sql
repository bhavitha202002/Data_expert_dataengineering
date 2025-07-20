/*
Incremental query for actors_history_scd: Write an "incremental" query that combines the previous year's SCD data with new incoming data from the actors table.
*/

CREATE TYPE scd_type AS 
(
start_date integer,
end_date integer,
quality_class quality_class,
is_active boolean
);


-- this is the actual incremental query
WITH
last_season_scd AS ( -- latest, current record for all actors
SELECT * FROM actors_history_scd
WHERE current_year = 2020
AND end_date = 2020
),
historical_scd AS ( -- historical records for all actors, one per period
SELECT
actorid,
start_date,
end_date,
quality_class,
is_active,
current_year
FROM actors_history_scd

WHERE current_year = 2020
AND end_date < 2020
),
this_season_data AS ( -- new incoming data
SELECT * FROM actors
WHERE year = 2021
),
unchanged_records AS ( -- records that didn't change between new data and latest records
SELECT
ts.actorid,
ls.start_date,
ts.year AS end_year,
ts.quality_class,
ts.is_active,
ls.current_year
-- for these records we increase `current_season` by 1
-- or in other words, we increase the range of the validity period
-- HINT: read `start_season` and `end_season` as `valid_from`, `valid_to`
FROM this_season_data ts
JOIN last_season_scd ls
ON ls.actorid = ts.actorid
WHERE ts.quality_class = ls.quality_class
AND ts.is_active = ls.is_active
),
-- actors with changed data
-- this one has 2 records per actors
-- one for previous and one for this period, (in this case 2020 and 2021)
changed_records AS (
SELECT
ts.actorid,
UNNEST(ARRAY[
ROW(
ls.start_date,
ls.end_date,
ls.quality_class,
ls.is_active
)::scd_type,
ROW(
ts.year,
ts.year,
ts.quality_class,
ts.is_active
)::scd_type
]) AS records,
ts.year
FROM this_season_data ts
LEFT JOIN last_season_scd ls
ON ls.actorid = ts.actorid
WHERE ts.quality_class <> ls.quality_class
OR ts.is_active <> ls.is_active
),
-- builds from previous CTE, just makes it more readable
unnested_changed_records AS (
SELECT
actorid,
(records).start_date,
(records).end_date,
(records).quality_class,
(records).is_active,
year
from changed_records
),
new_records AS ( -- new actors that were not in the dataset before
SELECT
ts.actorid,
ts.year AS start_date,
ts.year AS end_date,
ts.quality_class as quality_class,
ts.is_active as is_active,
ts.year as current_year
FROM this_season_data ts
LEFT JOIN last_season_scd ls
ON ts.actorid = ls.actorid

WHERE ls.actorid IS NULL
-- only include those actors that don't exist in last_season (ls)
)
SELECT * FROM historical_scd
UNION ALL
SELECT * FROM unchanged_records
UNION ALL
SELECT * FROM unnested_changed_records
UNION ALL
SELECT * FROM new_records

/*

Developing of SCD-2 involved the following steps:

1. Forming a base table which contains data year by year and containing cumulative data that can be helpful for building scd-2
2. Historical loading of scd-2 with the help of is_active, quality_class changes
3. Now for incremental-we take historical_scd, unchanged_records, changed_records, new records

