# Sling
https://slingdata.io/
<br>https://github.com/slingdata-io/sling-cli
> Sling is a Powerful Data Integration tool enabling seamless ELT operations as well as quality checks across files, databases, and storage systems.

## Structure
``` mermaid
flowchart LR
replication_config.yaml@{ shape: docs }-->factory
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
The factory will parse user defined yaml files representing connections and streams into dagster resources and assets.

### Translator
The translator will tell dagster how to translate sling concepts into dagster concepts, such as how a asset key is defined, or a automation condition.

### Resources
The resources will pass all the translated assets to the dagster runtime.

## Configs

### Connection Config
https://docs.slingdata.io/sling-cli/environment#sling-env-file-env.yaml
https://docs.dagster.io/api/libraries/dagster-sling#dagster_sling.SlingResource
``` yaml
connection:
  name: accounts_db
  type: postgres
  database: {SOURCE__ON_PREM_OLTP__CREDENTIALS__DATABASE: show}
  host: {SOURCE__ON_PREM_OLTP__CREDENTIALS__HOST: show}
  port: {SOURCE__ON_PREM_OLTP__CREDENTIALS__PORT: mask}
  user: {SOURCE__ON_PREM_OLTP__CREDENTIALS__USER: mask}
  password: {SOURCE__ON_PREM_OLTP__CREDENTIALS__PASSWORD: mask}
```

### Replication Config
https://docs.slingdata.io/concepts/replication
``` yaml
replication:
  env: {SLING_LOADED_AT_COLUMN: timestamp}
  source: accounts_db
  target: snowflake

  defaults:
    mode: incremental
    object: '{stream_schema_upper}.{stream_table_upper}'
    primary_key: [id]
    update_key: updated_at
    target_options:
      column_casing: snake
      add_new_columns: true
      adjust_column_type: true
    meta:
      dagster:
        automation_condition: "on_cron_no_deps"
        automation_condition_config: {"cron_schedule":"@daily", "cron_timezone":"utc"}
        freshness_lower_bound_delta: 1800

  streams:
    accounts_db.accounts:
      tags: ['contains_pii', 'my_tag']

```