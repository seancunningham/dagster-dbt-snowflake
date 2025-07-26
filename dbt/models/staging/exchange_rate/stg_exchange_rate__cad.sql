{{
  config(
    materialized = 'view',
    )
-}}

with cad as (
    select * from {{ source("exchange_rate", "cad") }}
),

renamed as (
    select
    {% for col in adapter.get_columns_in_relation(source("exchange_rate", "cad")) %}
        {{ col.name }} "{{ col.name.split("__", maxsplit=1)[-1].upper() -}}"
        {%- if not loop.last -%}
        ,
        {%- endif -%}
    {% endfor %}
    from cad
)

select * from renamed
