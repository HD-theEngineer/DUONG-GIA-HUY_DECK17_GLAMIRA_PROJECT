WITH stg_glamira__load AS (
    SELECT
        *
    FROM
        {{ref("stg_glamira__temp_raw_data")}}
)

, stg_glamira__extraction_cart_options AS (
    SELECT 
        current_url,
        product_option.option_id    AS option_id,
        product_option.option_label AS option_name,
        product_option.value_id     AS option_value_id,
        product_option.value_label  AS option_value_name
    FROM 
        stg_glamira__load
    LEFT JOIN 
        UNNEST(cart_products)   AS product
    LEFT JOIN 
        UNNEST(product.option)  AS product_option
)

, stg_glamira__alloy_filter AS(
    SELECT DISTINCT
        TRANSLATE(TRIM(LOWER(option_value_name)), '-_', '  ')   AS alloy
    FROM 
        stg_glamira__extraction_cart_options
    WHERE 
        option_name = 'alloy'
)

, stg_glamira__alloy_parsing AS (
    SELECT
        alloy                              AS alloy_name,
        REGEXP_EXTRACT(alloy, r'(\d{3,4})')     AS fineness,
        CASE 
            WHEN alloy LIKE '%stainless%'
                THEN CONCAT(
                    'Stainless Steel /', 
                    REGEXP_EXTRACT(
                        alloy, r'(\d{3,4})'),
                    ' Gold'
                    )
            WHEN alloy LIKE '%ceramic%' 
                THEN CONCAT(
                    'Ceramic / ', 
                    REGEXP_EXTRACT(
                        alloy, r'(\d{3,4})'),
                    ' Gold'
                    )
            WHEN alloy LIKE '%gold%' 
                THEN 'Gold'
            WHEN alloy LIKE '%silber%' 
                OR alloy LIKE '%silver%'
                THEN 'Silver'
            WHEN alloy LIKE '%platin%' 
                THEN 'Platinum'
            WHEN alloy LIKE '%palladium%' 
                THEN 'Palladium'
            ELSE NULL
        END                                     AS metal,
        CASE 
            WHEN alloy LIKE '%platin%'
                OR alloy LIKE '%palladium%'
                OR alloy LIKE '%silber%'
                OR alloy LIKE '%silver%'
                THEN 'White'
            WHEN alloy LIKE '%weiß gelb%'
                OR alloy LIKE '%white / yellow%'
                THEN 'White Yellow'
            WHEN alloy LIKE '%gelb weiß%'
                OR alloy LIKE '%yellow / white%'
                THEN 'Yellow White'
            WHEN alloy LIKE '%weiß rot%'
                OR alloy LIKE '%white / rose%'
                OR alloy LIKE '%white / red%'
                THEN 'White Rose'
            WHEN alloy LIKE '%rot weiß%'
                OR alloy LIKE'%rose / white%'
                OR alloy LIKE'%red / white%'
                THEN 'Rose White'
            WHEN alloy LIKE '%weiß%'
                OR alloy LIKE '%white%'
                THEN 'White'
            WHEN alloy LIKE '%rot%'
                OR alloy LIKE '%rose%'
                THEN 'Rose'
            WHEN alloy LIKE '%gelb%'
                OR alloy LIKE '%yellow%'
                THEN 'Yellow'
            ELSE NULL
        END                                     AS color
    FROM
        stg_glamira__alloy_filter
)

, stg_glamira__gen_key AS (
    SELECT
        FARM_FINGERPRINT(alloy_name)    AS alloy_key,
        alloy_name                      AS alloy_name,
        COALESCE(fineness, 'XNA')       AS alloy_fineness,
        COALESCE(metal, 'XNA')          AS alloy_material,
        COALESCE(color, 'XNA')          AS alloy_color
    FROM
        stg_glamira__alloy_parsing
    UNION ALL
    SELECT
        -1      AS alloy_key,
        'XNA'   AS alloy_name,
        'XNA'   AS alloy_fineness,
        'XNA'   AS alloy_material,
        'XNA'   AS alloy_color
)

SELECT
    *
FROM stg_glamira__gen_key

/*
, stg_glamira__alloy_parsing AS (
    SELECT
        alloy,
        REGEXP_EXTRACT(alloy, r'(\d{3,4})') AS fineness,
        alloy, r'(?i)\b(gold|platin|platinum|silver)\b') AS metal_word,
        TRIM(
            COALESCE(
                -- case: "585 Natural White Gold"  -> capture "Natural White"
                REGEXP_EXTRACT(alloy, r'(?i)\d{3,4}\s+([^\d/]+?)\s+(?:gold|platin|platinum)\b'),
                -- case: "Ceramic / 585 White Gold" (same as above)
                REGEXP_EXTRACT(alloy, r'(?i)/\s*\d{3,4}\s+([^\d/]+?)\s+(?:gold|platin|platinum)\b'),
                -- case: compound words like "Weiß-Rotgold" or "Gelbgold" -> capture part before "gold"
                REGEXP_EXTRACT(alloy, r'(?i)\b([^\d/\s]+(?:[-][^\d/\s]+)*)\s*(?:gold)\b'),
                -- case: explicit words before "platin"
                REGEXP_EXTRACT(alloy, r'(?i)\b([^\d/]+?)\s*(?:platin|platinum)\b'),
                -- fallback: remove known tokens (numbers, metal, slash) and trim remainder
                TRIM(REGEXP_REPLACE(alloy, r'(?i)(\d{3,4}|gold|platin|platinum|/)', ''))
            )
        ) AS color_raw
    FROM
        stg_glamira__alloy_filter
)

, stg_glamira__alloy_normalized AS (
    SELECT
        alloy,
        LOWER(CONCAT(COALESCE(metal_word, 'gold'), ' ', COALESCE(fineness, '')) ) AS metal_normalized,
        REGEXP_REPLACE(TRIM(color_raw), r'\s{2,}', ' ') AS color_extracted
    FROM
        stg_glamira__alloy_parsing
)

-- , stg_glamira__extract_option AS
*/