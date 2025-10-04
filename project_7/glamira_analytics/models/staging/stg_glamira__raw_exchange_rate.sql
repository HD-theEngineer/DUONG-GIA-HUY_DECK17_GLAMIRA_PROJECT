with source as (
        select * from {{ ref('exchange_rate') }}
  ),
  renamed as (
      select
          {{ adapter.quote("country_name") }},
        {{ adapter.quote("country_code") }},
        {{ adapter.quote("currency_symbol") }},
        {{ adapter.quote("currency_name") }},
        {{ adapter.quote("exchange_rate_to_usd") }}
      from source
  )
  select * from renamed
