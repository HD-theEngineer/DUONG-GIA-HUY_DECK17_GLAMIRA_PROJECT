WITH store_info AS (
    SELECT
        DISTINCT store_id AS store_key,
        CONCAT('store-', CAST(store_id AS STRING)) AS store_name
    FROM
        {{ ref("stg_glamira__temp_raw_data") }}
    ORDER BY 1
)

SELECT * FROM store_info