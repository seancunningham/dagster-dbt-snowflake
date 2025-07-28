{{
  config(
    materialized = "view",
    )
-}}

with usd as (
    select * from {{ source("exchange_rate", "usd") }}
),

renamed as (
    select
        usd__cad cad,
    from usd
)

select * from renamed
