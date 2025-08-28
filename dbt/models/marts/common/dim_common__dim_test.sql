{{-
    config(
        schema = "common",
        alias = "dim_test",
        materialized = 'table',
    )
-}}

select * from {{ ref("stg_accounts_db__accounts") }} --noqa:all
