WITH dim_stone__load AS (
    SELECT
        *
    FROM
        {{ref("stg_glamira__product_option_stone")}}
)

SELECT * FROM dim_stone__load