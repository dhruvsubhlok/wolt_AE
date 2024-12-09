CREATE TABLE IF NOT EXISTS dim_date (
   date DATE,
   year INTEGER,
   month INTEGER,
   month_name VARCHAR


);


WITH RECURSIVE date_cte AS (
   SELECT DATE('2020-01-01') AS date_key
   UNION ALL
   SELECT DATE_ADD(date_key, INTERVAL 1 DAY)
   FROM date_cte
   WHERE date_key < '2025-12-31'
)


INSERT INTO dim_date
SELECT
   date_key,
   YEAR(date_key) AS year,
   MONTH(date_key) AS month,
   MONTHNAME(date_key) AS month_name
FROM date_cte;