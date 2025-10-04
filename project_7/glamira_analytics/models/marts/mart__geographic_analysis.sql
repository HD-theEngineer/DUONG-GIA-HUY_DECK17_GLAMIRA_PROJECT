WITH mart__sales_order_line_total AS (
    SELECT
    *,
    product_amount * product_price_usd AS line_total
    FROM
        {{ref("fact_sales_order")}}
)

, mart__dim_location_load AS (
    SELECT
        *
    FROM
        {{ref("dim_location")}}
)

, mart__dim_user_load AS (
    SELECT
        *
    FROM
        {{ref("dim_user")}}
)

, mart__join_location AS (
    SELECT
        l.country_name AS country_name,
        o.line_total AS line_total,
        u.user_unique_email_address AS user_email,
        o.order_key AS order_key,
        o.product_key AS product_key
    FROM
        mart__sales_order_line_total o
        LEFT JOIN mart__dim_location_load l
        USING (location_key)
        LEFT JOIN mart__dim_user_load u
        USING (user_key)
    WHERE u.user_unique_email_address NOT LIKE '%glamira%'
)

SELECT
    country_name,
    product_key,
    SUM(line_total) AS revenue_per_country,
    COUNT(order_key) AS order_distribution_per_country,
    COUNT(user_email) AS user_distribution_per_country
FROM
    mart__join_location
GROUP BY
    country_name,
    product_key
HAVING country_name <> 'XNA'
ORDER BY
    revenue_per_country DESC