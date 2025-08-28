{{-
    config(
        schema = "common",
        alias = "fct_transactions",
        materialized = "view",
    )
-}}

with transactions as (
    select * from {{ ref("stg_transaction_db__transactions") }}
)

select * from transactions
