with source as (
        select * from {{ source('glamira', 'raw_crawl_products') }}
  ),
  renamed as (
      select
          {{ adapter.quote("url") }},
        {{ adapter.quote("product_id") }},
        {{ adapter.quote("name") }},
        {{ adapter.quote("sku") }},
        {{ adapter.quote("attribute_set_id") }},
        {{ adapter.quote("attribute_set") }},
        {{ adapter.quote("type_id") }},
        {{ adapter.quote("price") }},
        {{ adapter.quote("min_price") }},
        {{ adapter.quote("max_price") }},
        {{ adapter.quote("min_price_format") }},
        {{ adapter.quote("max_price_format") }},
        {{ adapter.quote("gold_weight") }},
        {{ adapter.quote("none_metal_weight") }},
        {{ adapter.quote("fixed_silver_weight") }},
        {{ adapter.quote("material_design") }},
        {{ adapter.quote("qty") }},
        {{ adapter.quote("collection") }},
        {{ adapter.quote("collection_id") }},
        {{ adapter.quote("product_type") }},
        {{ adapter.quote("product_type_value") }},
        {{ adapter.quote("category") }},
        {{ adapter.quote("category_name") }},
        {{ adapter.quote("store_code") }},
        {{ adapter.quote("show_popup_quantity_eternity") }},
        {{ adapter.quote("visible_contents") }},
        {{ adapter.quote("gender") }}

      from source
  )
  select * from renamed WHERE product_id IS NOT NULL