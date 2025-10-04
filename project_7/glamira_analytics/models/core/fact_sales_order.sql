{{
    config(
        materialized='incremental',
        unique_key='sales_order_key',
        incremental_strategy='merge',
        partition_by={
            "field": "update_date",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}


SELECT
    DATE(created_at_utc) AS update_date,
    sales_order_key,
    date_key,
    user_key,
    store_key,
    location_key,
    order_key,
    product_key,
    option_key,
    local_time,
    ip_address,
    currency_symbol,
    product_amount,
    product_unit_price,
    exchange_rate_to_usd,
    product_price_usd
FROM
    {{ref("stg_glamira__sales_order")}}

{% if is_incremental() %}

    WHERE created_at_utc > (
        SELECT 
            COALESCE(MAX(created_at_utc), TIMESTAMP('1900-01-01')) 
        FROM {{ this }}
    )

{% endif %}