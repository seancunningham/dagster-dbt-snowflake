{{
  config(
    materialized = "incremental",
    unique_key = "account_id",
    incremental_strategy="delete+insert",
    tags = ["contains_pii"],
    grants = {"select": []},
    meta = {
      "dagster": {
        "automation_condition": "eager",
        "freshness_check": {"lower_bound_delta_seconds": 129600}
      }
    }
  )
}}

with accounts as (
  select * from {{ ref("src_accounts_db__src_accounts") }}
)

select
  account_id,
  account_first_name,
  account_last_name,
  account_email,
  _loaded_at
from accounts

{% if is_incremental() -%}
  where _loaded_at >= coalesce((select max(_loaded_at) from {{ this }}), '1900-01-01')
{%- endif %}