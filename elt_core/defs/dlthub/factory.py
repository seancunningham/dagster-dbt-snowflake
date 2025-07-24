import os
from datetime import timedelta

import dlt
import dagster as dg
from dlt.extract.resource import DltResource
from dagster_dlt import dlt_assets, DagsterDltResource

from elt_core.defs.dlthub.translator import CustomDagsterDltTranslator



def dlt_assets_factory(resources, schema, kinds):
    asset_name = schema
    if bool(os.getenv("DAGSTER_IS_DEV_CLI")):
        user = os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME")
        schema = f"{schema}__{user}"

    @dlt.source()
    def source():
        for resource in resources:
            yield resource

    @dlt_assets(
        name=asset_name,
        dlt_source=source(),
        backfill_policy=dg.BackfillPolicy.single_run(),
        dagster_dlt_translator=CustomDagsterDltTranslator(),
        dlt_pipeline=dlt.pipeline(
            pipeline_name=asset_name,
            destination="snowflake",
            dataset_name=schema,
            progress="log",
        ),
    )
    def assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
        yield from dlt.run(context=context)

    deps = []
    for resource in resources:
        table_name = resource.table_name
        dep = dg.AssetSpec([asset_name, "src", table_name], kinds=kinds, group_name=asset_name)
        deps.append(dep)

    return assets, deps

dg.AssetsDefinition
def dlt_freshness_checks_factory(assets: list[dg.AssetsDefinition]) -> list[dg.AssetChecksDefinition]:
    freshness_checks = []
    for assets_definition in assets:
        for asset_spec in assets_definition.specs:
            try: # to get freshness check from tags, injected from meta in the custom translator
                if delta:= asset_spec.meta["dagster"]["freshness_lower_bound_delta"]:
                    last_update_freshness_check = dg.build_last_update_freshness_checks(
                                assets=[asset_spec.key],
                                lower_bound_delta=timedelta(minutes=float(delta))
                            )
                    freshness_checks.extend(last_update_freshness_check)
            except: ...

    return freshness_checks

def add_configs(resource: DltResource, meta:dict = None, tags:list = None) -> DltResource:
    resource.meta = meta
    resource.tags = tags
    return resource

