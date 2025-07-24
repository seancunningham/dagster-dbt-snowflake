```mermaid
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