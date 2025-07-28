# About
A Dagster project integrating dbt, Sling, dltHub, and Snowflake into a single data platform.
Includes stubs for powerBi, and AzureML, as well as Azure Keyvault to demonstrate
external integrations.


``` mermaid
flowchart LR
    subgraph Sling / dltHub 
        src-->raw
    end
    subgraph dbt 
        raw-->pii
        raw-->stg
        raw-->snp
        snp-->int
        stg-->int
        int-->mrt
    end
    subgraph PowerBI
        mrt-->sem
        sem-->exp
    end
    subgraph Snowpark
        mrt-->ml
        int-->ml
    end
```

# Setup
Requires a .env file with the following keys
```
TARGET=dev
DAGSTER_HOME=.\\.dagster_home
PYTHONLEGACYWINDOWSSTDIO=1
DBT_PROJECT_DIR=.\\dbt\\
PREPARE_IF_DEV=0

PROD__DESTINATION__DATABASE=raw
PROD__DESTINATION__HOST=
PROD__DESTINATION__ROLE=
PROD__DESTINATION__USER=
PROD__DESTINATION__PASSWORD=
PROD__DESTINATION__WAREHOUSE=

DEV__DESTINATION__DATABASE=_dev_raw
DEV__DESTINATION__HOST=
DEV__DESTINATION__ROLE=
DEV__DESTINATION__USER=
DEV__DESTINATION__PASSWORD=
DEV__DESTINATION__WAREHOUSE=compute_wh

ANY__SOURCE__DATABASE=
ANY__SOURCE__HOST=
ANY__SOURCE__PORT=
ANY__SOURCE__USER=
ANY__SOURCE__PASSWORD=
```