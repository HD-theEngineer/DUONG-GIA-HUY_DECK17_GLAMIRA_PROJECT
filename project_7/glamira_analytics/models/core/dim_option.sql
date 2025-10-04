WITH dim_option__load AS (
    SELECT
        option_key,
        stone_key,
        alloy_key
    FROM
        {{ref("stg_glamira__product_option")}}
)

SELECT * FROM dim_option__load