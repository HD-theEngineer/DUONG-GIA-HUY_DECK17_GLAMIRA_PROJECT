WITH dim_alloy__load AS (
    SELECT
        *
    FROM
        {{ref("stg_glamira__product_option_alloy")}}
)

SELECT * FROM dim_alloy__load