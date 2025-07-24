# dltHub
https://dlthub.com/
<br>https://github.com/dlt-hub/dlt

>dlt is the most popular production-ready Python library for moving data. It loads data from various and often messy data sources into well-structured, live datasets.
>
>Unlike other non-Python solutions, with dlt, there's no need to use any backends or containers. We do not replace your data platform, deployments, or security models. Simply import dlt in your favorite AI code editor, or add it to your Jupyter Notebook. You can load data from any source that produces Python data structures, including APIs, files, databases, and more.

## Structure
``` mermaid
flowchart LR
dlt_source.py@{ shape: docs }-->factory
translator --> factory
factory@{ shape: procs } --> assets_definition
    subgraph definitions
        assets_definition
        resource
    end
context
assets_definition@{ shape: doc } --> run
resource --> run
context --> run
```
### Factory
The factory will parse user defined python scripts into dagster resources and assets.

### Translator
The translator will tell dagster how to translate dltHub concepts into dagster concepts, such as how a asset key is defined, or a automation condition.

### Resources
The resources will pass all the translated assets to the dagster runtime.
