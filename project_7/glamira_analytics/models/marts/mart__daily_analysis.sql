WITH mart__sales_order_line_total AS (
    SELECT
    *,
    product_amount * product_price_usd AS line_total
    FROM
        {{ref("fact_sales_order")}}
)

, mart__dim_date_load AS (
    SELECT
        *
    FROM
        {{ref("dim_date")}}
)

, mart__dim_user_load AS (
    SELECT
        *
    FROM
        {{ref("dim_user")}}
)

, mart__join_date_user AS (
    SELECT
        d.date AS date,
        o.line_total AS line_total,
        u.user_unique_email_address AS user_email,
        o.order_key AS order_key
    FROM
        mart__sales_order_line_total o
        LEFT JOIN mart__dim_date_load d
        USING (date_key)
        LEFT JOIN mart__dim_user_load u
        USING (user_key)
    WHERE
        u.user_unique_email_address NOT LIKE '%glamira%'
)

SELECT
    date,
    SUM(line_total) AS daily_revenue,
    'USD' AS currency,
    COUNT(order_key) AS daily_order_count
FROM
    mart__join_date_user
GROUP BY
    date
HAVING date IS NOT NULL
ORDER BY
    date