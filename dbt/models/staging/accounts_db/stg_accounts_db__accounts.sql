{{
  config(
    materialized = "incremental",
    unique_key = "account_id",
    incremental_strategy="delete+insert",
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
  sha2(account_first_name, 256) account_first_name,
  sha2(account_last_name, 256)  account_last_name,
  sha2(account_email, 256)      account_email,
  indvidual_party_key,
  _loaded_at
from accounts

{% if is_incremental() -%}
  where _loaded_at >= coalesce((select max(_loaded_at) from {{ this }}), '1900-01-01')
{%- endif %}