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
        {% for col in adapter.get_columns_in_relation(source("exchange_rate", "cad")) -%}
            {% if not col.name.lower().startswith("_dlt") %}
            {{- col.name }} "{{ col.name.split("__", maxsplit=1)[-1].upper() }}",
            {% endif -%}
        {% endfor %}
        {{- dlt_loaded_at() }} _loaded_at
    from cad
)

select * from renamed
