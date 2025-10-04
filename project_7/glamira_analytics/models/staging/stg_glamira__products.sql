-- LOAD ALL cart_products FROM RAW DATA
WITH stg_glamira__cart_load AS (
    SELECT
        cart_products               AS cart_products
    FROM
        {{ref("stg_glamira__temp_raw_data")}}
)

-- EXTRACT product_id FROM UNNEST cart_products
, stg_glamira__cart_extract_product_id AS (
    SELECT
        cart.product_id             AS product_key
    FROM
        stg_glamira__cart_load r,
        UNNEST(r.cart_products) AS cart
)

-- LOAD ALL product_id (not in cart) FROM RAW DATA
, stg_glamira__product_load AS (
    SELECT DISTINCT
        CAST(
            product_id AS INTEGER
        )                           AS product_key
    FROM
        {{ref("stg_glamira__temp_raw_data")}}
)

-- JOIN 2 TABLE OF product_id (not unique)
, stg_glamira__join_product_id AS (
    SELECT DISTINCT
        product_key                 AS product_key
    FROM
        stg_glamira__cart_extract_product_id
    UNION ALL
    SELECT
        *
    FROM
        stg_glamira__product_load
)

-- FILTER THE LIST WITH ONLY DISTINCT VALUE
, stg_glamira__distinct_product_key AS (
    SELECT DISTINCT
        *
    FROM
        stg_glamira__join_product_id
)

-- LOAD ALL product data FROM CRAWLING
, stg_glamira__products_crawl_rename AS (
    SELECT
        product_id                  AS product_key,
        ARRAY_AGG(name)             AS product_name,
        sku                         AS product_sku_name,
        attribute_set_id            AS attribute_set_id,
        attribute_set               AS attribute_set_name,
        CAST(
            LEFT(collection_id, 4) AS INTEGER
        )                           AS collection_id,
        collection                  AS collection_name,
        CAST(
            product_type_value AS INTEGER
        )                           AS product_type_id,
        product_type                AS product_type_name,
        CAST(
            category AS INTEGER
        )                           AS category_id,
        ARRAY_AGG(category_name)    AS category_name,
        gender
    FROM
        {{ ref("stg_glamira__raw_crawl_products")}}
    GROUP BY
        product_key,
        product_sku_name,
        attribute_set_id,
        attribute_set_name,
        collection_id,
        collection_name,
        product_type_id,
        product_type_name,
        category_id,
        gender
)

-- ENRICH PRE-POPULATED product_id LIST (not UNIQUE) WITH product data
, stg_glamira__products_enrich AS (
    SELECT
        *
    FROM
        stg_glamira__distinct_product_key
    FULL OUTER JOIN
        stg_glamira__products_crawl_rename
    USING(product_key)
)

-- STANDARDIZE ALL DATA FORMAT TO LATER HANDLE EASIER
, stg_glamira__products_handle_invalid AS(
    SELECT
        product_key,
        CASE 
            WHEN ARRAY_LENGTH(product_name) = 0
                THEN product_name[SAFE_OFFSET(1)]
            ELSE product_name[SAFE_OFFSET(0)]
            END 
        AS product_name,
        product_sku_name,
        attribute_set_id,
        attribute_set_name,
        collection_id,
        CASE 
            WHEN TRIM(collection_name) = '' OR TRIM(collection_name) = '-' 
                THEN NULL
            ELSE TRIM(collection_name)
            END 
        AS collection_name,
        product_type_id,
        product_type_name,
        category_id,
        CASE 
            WHEN ARRAY_LENGTH(category_name) = 0
                THEN category_name[SAFE_OFFSET(1)]
            ELSE category_name[SAFE_OFFSET(0)]
            END 
        AS category_name,
        CASE 
            WHEN gender = 'false' THEN NULL
            ELSE gender 
            END 
        AS gender
    FROM
        stg_glamira__products_enrich
)

SELECT
    *
FROM
    stg_glamira__products_handle_invalid