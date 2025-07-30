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