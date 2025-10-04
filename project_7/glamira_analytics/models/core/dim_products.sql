WITH dim_product__load AS (
    SELECT
        *
    FROM
        {{ ref('stg_glamira__products')}}
)

, dim_products__null_handle AS (
    SELECT
        COALESCE(product_key, -1)           AS product_key,
        COALESCE(product_name, 'XNA')       AS product_name,
        COALESCE(product_sku_name, 'XNA')   AS product_sku_name,
        COALESCE(attribute_set_id, -1)      AS attribute_set_id,
        COALESCE(attribute_set_name, 'XNA') AS attribute_set_name,
        COALESCE(collection_id, -1)         AS collection_id,
        COALESCE(collection_name, 'XNA')    AS collection_name,
        COALESCE(product_type_id, -1)       AS product_type_id,
        COALESCE(product_type_name, 'XNA')  AS product_type_name,
        COALESCE(category_id, -1)           AS category_id,
        COALESCE(category_name, 'XNA')      AS category_name,
        COALESCE(gender, 'XNA')             AS gender,
    FROM
        dim_product__load
)

SELECT
   *
FROM
    dim_products__null_handle
