with source as (
        select * from {{ source('glamira', 'temp_raw_data') }}
  ),
  renamed as (
      select
        {{ adapter.quote("_id") }},
        {{ adapter.quote("time_stamp") }},
        {{ adapter.quote("ip") }},
        {{ adapter.quote("user_agent") }},
        {{ adapter.quote("resolution") }},
        {{ adapter.quote("user_id_db") }},
        {{ adapter.quote("device_id") }},
        {{ adapter.quote("api_version") }},
        {{ adapter.quote("store_id") }},
        {{ adapter.quote("local_time") }},
        {{ adapter.quote("show_recommendation") }},
        {{ adapter.quote("current_url") }},
        {{ adapter.quote("referrer_url") }},
        {{ adapter.quote("email_address") }},
        {{ adapter.quote("recommendation") }},
        {{ adapter.quote("utm_source") }},
        {{ adapter.quote("utm_medium") }},
        {{ adapter.quote("collection") }},
        {{ adapter.quote("product_id") }},
        {{ adapter.quote("option") }},
        {{ adapter.quote("order_id") }},
        {{ adapter.quote("cart_products") }},
        {{ adapter.quote("cat_id") }},
        {{ adapter.quote("collect_id") }},
        {{ adapter.quote("key_search") }},
        {{ adapter.quote("price") }},
        {{ adapter.quote("currency") }},
        {{ adapter.quote("is_paypal") }},
        {{ adapter.quote("viewing_product_id") }},
        {{ adapter.quote("recommendation_product_id") }},
        {{ adapter.quote("recommendation_product_position") }},
        {{ adapter.quote("recommendation_clicked_position") }}

      from source
  )

select * from renamed --WHERE collection = 'checkout_success'