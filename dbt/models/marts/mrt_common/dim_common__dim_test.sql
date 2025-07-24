{{-
  config(
    materialized = 'table',
    )
-}}

select * from {{ ref("stg_accounts_db__accounts") }}