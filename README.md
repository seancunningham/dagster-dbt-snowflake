# About
A Dagster project integrating dbt, Sling, dltHub, and Snowflake into a single data platform.
Includes stubs for powerBi, and AzureML, as well as Azure Keyvault to demonstrate
external integrations.


``` mermaid
flowchart
    subgraph elt core
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

DESTINATION__SNOWFLAKE__DATABASE=
DESTINATION__SNOWFLAKE__HOST=
DESTINATION__SNOWFLAKE__ROLE=
DESTINATION__SNOWFLAKE__USER=
DESTINATION__SNOWFLAKE__WAREHOUSE=
DESTINATION__SNOWFLAKE__PASSWORD=

SOURCE_DATABASE=
SOURCE_HOST=
SOURCE_PORT=
SOURCE_USER=
SOURCE_PASSWORD=
```