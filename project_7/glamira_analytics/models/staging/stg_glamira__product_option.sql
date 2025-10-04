WITH stg_glamira__load AS (
    SELECT
        *
    FROM
        {{ref("stg_glamira__temp_raw_data")}}
    WHERE
        collection = 'checkout_success'
)

, stg_glamira__extract_cart AS (
    SELECT 
        product.product_id AS product_key,
        product.option[SAFE_OFFSET(0)].value_label AS attribute_1,
        product.option[SAFE_OFFSET(1)].value_label AS attribute_2
    FROM 
        stg_glamira__load
    LEFT JOIN 
        UNNEST(cart_products) AS product
)

, stg_glamira__standardize AS (
    SELECT
        CASE 
            WHEN attribute_2 IS NULL THEN NULL
            ELSE attribute_1
        END AS stone_option,
        CASE 
            WHEN attribute_2 IS NULL THEN attribute_1
            ELSE attribute_2
        END AS alloy_option,
    FROM
        stg_glamira__extract_cart
)

, stg_glamira__unique_option_pair AS (
    SELECT DISTINCT
        TRANSLATE(
            CASE
                WHEN LOWER(TRIM(stone_option)) LIKE '%saphire%'
                    THEN 'sapphire'
                ELSE LOWER(TRIM(stone_option))
            END
        , '-_', '  ')                               AS stone_option,
        TRANSLATE(
            CASE
                WHEN LOWER(TRIM(alloy_option)) LIKE '%saphire%'
                    THEN 'sapphire'
                ELSE LOWER(TRIM(alloy_option))
            END
        , '-_', '  ')                                AS alloy_option
    FROM stg_glamira__standardize
)

, stg_glamira__null_handle AS (
    SELECT DISTINCT
        COALESCE(stone_option, 'XNA') AS stone_option,
        COALESCE(alloy_option, 'XNA') AS alloy_option
    FROM
        stg_glamira__unique_option_pair
)

, stg_glamira__load_alloy AS (
    SELECT
        alloy_key,
        alloy_name
    FROM
        {{ref("stg_glamira__product_option_alloy")}}
)

, stg_glamira__load_stone AS (
    SELECT
        stone_key,
        stone_name
    FROM
        {{ref("stg_glamira__product_option_stone")}}
)

, stg_glamira__join_stone_alloy AS (
    SELECT
        o.stone_option  AS stone_option,
        s.stone_key     AS stone_key,
        o.alloy_option  AS alloy_option,
        a.alloy_key     AS alloy_key
    FROM
        stg_glamira__null_handle o
    LEFT JOIN
        stg_glamira__load_stone s
        ON o.stone_option = s.stone_name
    LEFT JOIN
        stg_glamira__load_alloy a
        ON o.alloy_option = a.alloy_name
)

, stg_glamira__gen_key AS (
    SELECT
        CASE 
            WHEN stone_key = -1 AND alloy_key = -1 
                THEN -1
            ELSE FARM_FINGERPRINT(stone_key || alloy_key)
        END                                         AS option_key,
        COALESCE(stone_key, -1)                     AS stone_key,
        COALESCE(alloy_key, -1)                     AS alloy_key,
        COALESCE(stone_option, 'XNA')               AS stone_option,
        COALESCE(alloy_option, 'XNA')               AS alloy_option
    FROM
        stg_glamira__join_stone_alloy
)

SELECT * FROM stg_glamira__gen_key