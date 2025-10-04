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

, mart__dim_products_load AS (
    SELECT
        *
    FROM
        {{ref("dim_products")}}
)

, mart__revenue_by_product AS (
    SELECT
        product_key,
        product_name,
        SUM(line_total) AS revenue_by_product
    FROM
        mart__sales_order_line_total o
        LEFT JOIN mart__dim_products_load p
        USING (product_key)
    GROUP BY
        product_key,
        product_name
)

SELECT
    *
FROM
    mart__revenue_by_product