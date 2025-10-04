with source as (
        select * from {{ source('glamira', 'raw_ip2location') }}
  ),
  renamed as (
      select
          {{ adapter.quote("ip") }},
          {{ adapter.quote("country_short") }},
          {{ adapter.quote("country_long") }},
          {{ adapter.quote("region") }},
          {{ adapter.quote("city") }},
          {{ adapter.quote("latitude") }},
          {{ adapter.quote("longitude") }},
          {{ adapter.quote("zipcode") }}

      from source
  )
  select * from renamed
    