WITH raw_timeline AS (
  SELECT
    MAX(EXTRACT(DATE FROM time_stamp)) as last_date,
    MIN(EXTRACT(DATE FROM time_stamp)) as first_date
  FROM
    {{ref("stg_glamira__temp_raw_data")}}
)
, dates AS (
  SELECT day AS date
  FROM UNNEST(GENERATE_DATE_ARRAY((SELECT first_date FROM raw_timeline), (SELECT last_date FROM raw_timeline), INTERVAL 1 DAY)) AS day
)

, dim_date__modified AS (
  SELECT
    CAST(FORMAT_DATE('%Y%m%d', date) AS INTEGER) AS date_key,
    date,
    EXTRACT(DAY FROM date) AS day,
    FORMAT_DATE('%A', date) AS day_name,
    EXTRACT(ISOWEEK FROM date) AS week_number,
    EXTRACT(MONTH FROM date) AS month,
    FORMAT_DATE('%B', date) AS month_name,
    EXTRACT(YEAR FROM date) AS year,
    CASE WHEN EXTRACT(DAYOFWEEK FROM date) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend
  FROM dates
)

, dim_date__invalid_handle AS (
  SELECT
    *
  FROM
    dim_date__modified
  UNION ALL
  SELECT
    -1 AS date_key,
    CAST(NULL AS DATE) AS date,
    -1 AS day,
    'XNA' AS day_name,
    -1 AS week_number,
    -1 AS month,
    'XNA' AS month_name,
    -1 AS year,
    CAST(NULL AS BOOLEAN) AS is_weekend,
)

SELECT
  *
FROM
  dim_date__invalid_handle