WITH stg_glamira__order_rename AS (
    SELECT
        CAST(
            FORMAT_DATE(
                '%Y%m%d', EXTRACT(DATE FROM r.time_stamp)
            ) 
        AS INTEGER)                                                         AS date_key,
        SAFE_CAST(local_time AS DATETIME)                                   AS local_time,
        CASE
            WHEN TRIM(r.user_id_db) = '' 
                OR TRIM(r.user_id_db) = '-' 
                THEN NULL
            ELSE SAFE_CAST(r.user_id_db AS INTEGER)
        END                                                                 AS user_key,
        SAFE_CAST(r.store_id AS INTEGER)                                    AS store_key,
        r.ip                                                                AS ip_address,
        UPPER(REGEXP_EXTRACT(current_url, r'\.([a-z]+)/'))                  AS country_short_name,
        SAFE_CAST(SAFE_CAST(TRIM(r.order_id) AS FLOAT64) AS INTEGER)        AS order_key,
        SAFE_CAST(cart.product_id AS INTEGER)                               AS product_key,
        SAFE_CAST(cart.amount AS INTEGER)                                   AS product_amount,
        SAFE_CAST(
            CASE
                WHEN TRIM(cart.price) = '' OR cart.price IS NULL THEN '0'
                WHEN cart.price LIKE '%,%' AND cart.price NOT LIKE '%.%' 
                    THEN REPLACE(cart.price, ',', '.')
                WHEN STRPOS(cart.price, ',') > STRPOS(cart.price, '.')
                    THEN REPLACE(REPLACE(cart.price, '.', ''), ',', '.')
                WHEN STRPOS(cart.price, '.') > STRPOS(cart.price, ',')
                    THEN REPLACE(cart.price, ',', '')
                ELSE cart.price
            END
        AS FLOAT64)                                                         AS product_price,
        TRIM(
            REGEXP_REPLACE(
                cart.currency,r"[0-9,\.\s'’]+",''
            )
        )                                                                   AS currency_symbol,
        cart.option[SAFE_OFFSET(0)].value_label                             AS product_option_1,
        cart.option[SAFE_OFFSET(1)].value_label                             AS product_option_2
    FROM
        {{ ref("stg_glamira__temp_raw_data")}} r,
        UNNEST(r.cart_products) AS cart
    WHERE collection = 'checkout_success'
)

, stg_glamira__location_key_load AS (
    SELECT DISTINCT
        location_key AS location_key,
        ip_address AS ip_address
    FROM
        {{ ref('stg_glamira__location') }}
)

, stg_glamira__enrich_location_key AS (
    SELECT
        r.*,
        l.location_key AS location_key
    FROM
        stg_glamira__order_rename r
    LEFT JOIN
        stg_glamira__location_key_load l
    USING (ip_address)
)

, stg_glamira__value_standardize AS (
    SELECT
        date_key,
        local_time,
        user_key,
        ip_address,
        store_key,
        location_key,
        country_short_name,
        order_key,
        product_key,
        CASE 
            WHEN product_option_2 IS NULL THEN NULL
            ELSE product_option_1
        END AS stone_option,
        CASE 
            WHEN product_option_2 IS NULL THEN product_option_1
            ELSE product_option_2
        END AS alloy_option,
        product_amount,
        product_price,
        CASE 
            WHEN country_short_name = 'MX' AND currency_symbol = 'MXN$' THEN 'MXN'
            WHEN country_short_name = 'AR' AND currency_symbol = '$' THEN 'AR$'
            WHEN country_short_name = 'HK' AND currency_symbol IN ('HKD$','$') THEN 'HK$'
            WHEN country_short_name = 'NO' AND currency_symbol = 'kr' THEN 'NOkr'
            WHEN country_short_name = 'SE' AND currency_symbol = 'kr' THEN 'SEKr'
            WHEN country_short_name = 'DK' AND currency_symbol = 'kr' THEN 'DKkr'
            WHEN country_short_name IS NULL AND currency_symbol = 'kr' THEN 'SEKr'
            WHEN country_short_name = 'CN' AND currency_symbol = '￥' THEN 'CN￥'
            WHEN country_short_name = 'JP' AND currency_symbol = '￥' THEN 'JP￥'
            WHEN country_short_name = 'DE' AND TRIM(currency_symbol) = '' THEN '€'
            WHEN country_short_name = 'AE' AND TRIM(currency_symbol) = '' THEN 'AED'
            WHEN country_short_name = 'ZA' AND TRIM(currency_symbol) = '' THEN 'ZAR'
            WHEN country_short_name = 'RO' AND TRIM(currency_symbol) = '' THEN 'RON'
            WHEN country_short_name IN ('EC','PR') AND TRIM(currency_symbol) = 'USD$' THEN '$'
            ELSE currency_symbol
        END AS currency_symbol
    FROM
        stg_glamira__enrich_location_key
)

, stg_glamira__product_grouping AS (
    SELECT
        date_key                                    AS date_key,
        user_key                                    AS user_key,
        store_key                                   AS store_key,
        location_key                                AS location_key,
        order_key                                   AS order_key,
        product_key                                 AS product_key,
        local_time                                  AS local_time,
        ip_address                                  AS ip_address,
        TRANSLATE(
            CASE
                WHEN LOWER(TRIM(stone_option)) IS NULL
                    THEN 'XNA'
                WHEN LOWER(TRIM(stone_option)) LIKE '%saphire%'
                    THEN 'sapphire'
                ELSE LOWER(TRIM(stone_option))
            END
        , '-_', '  ')                               AS stone_option,
        TRANSLATE(
            CASE
                WHEN LOWER(TRIM(alloy_option)) IS NULL
                    THEN 'XNA'
                ELSE LOWER(TRIM(alloy_option))
            END
        , '-_', '  ')                               AS alloy_option,
        currency_symbol                             AS currency_symbol,
        SUM(product_amount)                         AS product_amount,
        AVG(
            CASE
                WHEN product_price = 0 THEN NULL
                ELSE product_price
            END
        )                                           AS product_unit_price
    FROM
        stg_glamira__value_standardize
    GROUP BY
        date_key,
        user_key,
        ip_address,
        store_key,
        location_key,
        order_key,
        product_key,
        local_time,
        stone_option,
        alloy_option,
        currency_symbol
)

, stg_glamira__exchange_load AS (
    SELECT DISTINCT
        TRIM(REGEXP_REPLACE(currency_symbol, r"[0-9,\s'’]+",''))            AS currency_symbol,
        SAFE_CAST(exchange_rate_to_usd AS FLOAT64)                          AS exchange_rate_to_usd
    FROM
        {{ ref("stg_glamira__raw_exchange_rate") }}
    WHERE
        currency_symbol <> 'RD$'
)

, stg_glamira__join_exchange_rate AS (
    SELECT
        o.date_key                                      AS date_key,
        o.user_key                                      AS user_key,
        o.store_key                                     AS store_key,
        o.location_key                                  AS location_key,
        o.order_key                                     AS order_key,
        o.product_key                                   AS product_key,
        o.local_time                                    AS local_time,
        o.ip_address                                    AS ip_address,
        o.stone_option                                  AS stone_option,
        o.alloy_option                                  AS alloy_option,
        o.currency_symbol                               AS currency_symbol,
        o.product_amount                                AS product_amount,
        o.product_unit_price                            AS product_unit_price,
        e.exchange_rate_to_usd                          AS exchange_rate_to_usd,
        o.product_unit_price * e.exchange_rate_to_usd   AS product_price_usd
    FROM
        stg_glamira__product_grouping o
    LEFT JOIN
        stg_glamira__exchange_load e
    USING (currency_symbol)
)

, stg_glamira__option_load AS (
    SELECT
        *
    FROM
        {{ ref("stg_glamira__product_option") }}
)

, stg_glamira__join_option AS (
    SELECT
        a.date_key                                      AS date_key,
        a.user_key                                      AS user_key,
        a.store_key                                     AS store_key,
        a.location_key                                  AS location_key,
        a.order_key                                     AS order_key,
        a.product_key                                   AS product_key,
        o.option_key                                    AS option_key,
        a.local_time                                    AS local_time,
        a.ip_address                                    AS ip_address,
        a.currency_symbol                               AS currency_symbol,
        a.product_amount                                AS product_amount,
        a.product_unit_price                            AS product_unit_price,
        a.exchange_rate_to_usd                          AS exchange_rate_to_usd,
        a.product_price_usd                             AS product_price_usd
    FROM
        stg_glamira__join_exchange_rate a
    LEFT JOIN
        stg_glamira__option_load o
    ON (a.stone_option = o.stone_option AND a.alloy_option = o.alloy_option)
)

, stg_glamira__order_gen_key AS (
SELECT
    FARM_FINGERPRINT(
        location_key || 
        order_key || 
        product_key || 
        FORMAT_TIME(
            '%H%M%S', EXTRACT(TIME FROM local_time)
        )
    ) AS sales_order_key,
    *
FROM
    stg_glamira__join_option
)

, fact_sales_order__null_handle AS (
    SELECT
        COALESCE(sales_order_key, -1)                           AS sales_order_key,
        COALESCE(date_key, -1)                                  AS date_key,
        COALESCE(user_key, -1)                                  AS user_key,
        COALESCE(store_key, -1)                                 AS store_key,
        COALESCE(location_key, -1)                              AS location_key,
        COALESCE(order_key, -1)                                 AS order_key,
        COALESCE(product_key, -1)                               AS product_key,
        COALESCE(option_key, -1)                                AS option_key,
        COALESCE(local_time, DATETIME '1900-01-01 00:00:00')    AS local_time,
        COALESCE(ip_address, 'XNA')                             AS ip_address,
        COALESCE(currency_symbol, 'XNA')                        AS currency_symbol,
        COALESCE(product_amount, 0)                             AS product_amount,
        COALESCE(product_unit_price, 0)                         AS product_unit_price,
        COALESCE(exchange_rate_to_usd, 0)                       AS exchange_rate_to_usd,
        COALESCE(product_price_usd, 0)                          AS product_price_usd
    FROM
        stg_glamira__order_gen_key
    UNION ALL
    SELECT
        -1                              AS sales_order_key,
        -1                              AS date_key,
        -1                              AS user_key,
        -1                              AS store_key,
        -1                              AS location_key,
        -1                              AS order_key,
        -1                              AS product_key,
        -1                              AS option_key,
        DATETIME('1900-01-01 00:00:00') AS local_time,
        'XNA'                           AS ip_address,
        'XNA'                           AS currency_symbol,
        0                               AS product_amount,
        0                               AS product_unit_price,
        0                               AS exchange_rate_to_usd,
        0                               AS product_price_usd
)

SELECT 
    CURRENT_TIMESTAMP() AS created_at_utc,
    * 
FROM 
    fact_sales_order__null_handle