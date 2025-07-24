{{
  config(
    materialized = "ephemeral",
    tags = ["contains_pii"]
  )
}}

with accounts as (
  select * from {{ source("accounts_db", "accounts") }}
)

select
  id                             account_id,
  trim(lower(first_name))        account_first_name,
  trim(lower(last_name))         account_last_name,
  trim(lower(email))             account_email,
  party_key                      indvidual_party_key,
  {{ pst_to_utc("updated_at") }} updated_at,
  _sling_loaded_at               _loaded_at
from accounts