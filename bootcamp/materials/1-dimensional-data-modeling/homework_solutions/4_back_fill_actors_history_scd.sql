/*
Backfill query for actors_history_scd: Write a "backfill" query that can populate the entire actors_history_scd table in a single query.
*/


INSERT INTO actors_history_scd
WITH
with_previous as (
SELECT
actorid,
year,
quality_class,
is_active,
LAG(quality_class, 1) OVER(PARTITION BY actorid ORDER BY year) AS
previous_quality_class,
LAG(is_active, 1) OVER(PARTITION BY actorid ORDER BY year) AS
previous_is_active
FROM actors
WHERE year <= 2020 --Loading till 2020 so that for 2021, will load in incremental
),
with_indicators AS (
SELECT
*,
CASE
WHEN quality_class <> previous_quality_class THEN 1
WHEN is_active <> previous_is_active THEN 1
ELSE 0
END AS change_indicator
FROM with_previous),
with_streaks AS (
SELECT
*,
SUM(change_indicator) OVER(PARTITION BY actorid ORDER BY year) AS
streak_identifier
FROM with_indicators

)
Select actorid,
min(year) as start_date,
max(year) as end_date,
quality_class,
is_active,
2021 as current_year
From with_streaks
Group by actorid, streak_identifier, is_active, quality_class

select * from actors_history_scd
update actors_history_scd set current_year = 2020;