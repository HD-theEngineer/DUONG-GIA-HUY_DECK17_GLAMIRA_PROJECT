WITH stg_glamira__load AS (
    SELECT
        *
    FROM
        {{ref("stg_glamira__temp_raw_data")}}
)

, stg_glamira__extraction_cart_options AS (
    SELECT 
        product.product_id          AS product_key,
        product_option.option_id    AS option_id,
        product_option.option_label AS option_name,
        product_option.value_id     AS option_value_id,
        product_option.value_label  AS option_value_name
    FROM 
        stg_glamira__load
    LEFT JOIN 
        UNNEST(cart_products) AS product
    LEFT JOIN 
        UNNEST(product.option) AS product_option
)

, stg_glamira__stone_filter AS (
    SELECT DISTINCT
        TRANSLATE(
            CASE
                WHEN LOWER(TRIM(option_value_name)) LIKE '%saphire%'
                    THEN 'sapphire'
                ELSE LOWER(TRIM(option_value_name))
            END
        , '-_', '  ') AS stone_name
    FROM 
        stg_glamira__extraction_cart_options
    WHERE 
        option_name = 'diamond'
)

, stg_glamira__gen_key AS (
    SELECT
        FARM_FINGERPRINT(stone_name)    AS stone_key,
        stone_name                      AS stone_name
    FROM
        stg_glamira__stone_filter
    UNION ALL
    SELECT
        -1      AS stone_key,
        'XNA'   AS stone_name
)

SELECT
    *
FROM stg_glamira__gen_key