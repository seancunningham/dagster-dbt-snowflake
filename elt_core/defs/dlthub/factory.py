from datetime import timedelta
from typing import Any, Generator, Sequence
from functools import cache

import dlt
import dagster as dg
from dlt.extract.resource import DltResource
from dagster_dlt import dlt_assets, DagsterDltResource
from dlt.extract.resource import DltResource
from dagster_dlt.dlt_event_iterator import DltEventType

from .translator import CustomDagsterDltTranslator



class ConfigurableDltResource(DltResource):
    meta: dict | None
    tags: list | None
    kinds: set | None

def convert_to_configurable_dlt_resource(
        dlt_resource: DltResource,
        meta: dict | None,
        tags: list[str] | None,
        kinds: set[str] | None) -> ConfigurableDltResource:
    
    dlt_resource.tags = tags # type: ignore
    dlt_resource.meta = meta # type: ignore
    dlt_resource.kinds = kinds # type: ignore
    return dlt_resource # type: ignore


def config(resource: DltResource,
        meta:dict|None=None,
        tags:list[str]|None = None,
        kinds:set[str]|None=None) -> ConfigurableDltResource:
    
    resource = convert_to_configurable_dlt_resource(resource, meta, tags, kinds)
    return resource # type: ignore

@cache
def build_dlt_definitions(resources: list[ConfigurableDltResource]) -> dg.Definitions:

    assets_definitions = []
    freshness_checks = []
    for resource in resources:
        assets = _build_assets(resource)
        assets_definitions.extend(assets)

        if last_update_freshness_check := _build_freshness_checks(resource):
            freshness_checks.extend(last_update_freshness_check)

    return dg.Definitions(
        assets=assets_definitions,
        asset_checks=freshness_checks
    )


def _build_assets(resource: ConfigurableDltResource) -> list[dg.AssetsDefinition | dg.AssetSpec]:
        schema, table = resource.name.split(".")
        
        @dlt.source()
        def source() -> Generator[DltResource, Any, None]:
            yield resource

        @dlt_assets(
            name=f"{schema}__{table}",
            op_tags={"tags": resource.tags},
            dlt_source=source(),
            backfill_policy=dg.BackfillPolicy.single_run(),
            dagster_dlt_translator=CustomDagsterDltTranslator(),
            dlt_pipeline=dlt.pipeline(
                pipeline_name=table,
                destination="snowflake",
                dataset_name=schema,
                progress="log",
            ),
        )
        def assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource) -> Generator[DltEventType, Any, None]:
            yield from dlt.run(context=context)

        # deps
        dep = dg.AssetSpec([schema, "src", table], kinds=resource.kinds, group_name=schema)

        return [assets, dep]

def _build_freshness_checks(resource: ConfigurableDltResource) -> Sequence[dg.AssetChecksDefinition] | None:
    schema, table = resource.name.split(".")
    if meta := resource.meta:
        if dagster := meta.get("dagster", {}):
            if delta := dagster.get("freshness_lower_bound_delta_seconds"):
                asset_key = dg.AssetKey([schema, "raw", table])
                last_update_freshness_check = dg.build_last_update_freshness_checks(
                            assets=[asset_key],
                            lower_bound_delta=timedelta(seconds=float(delta))
                        )
                return last_update_freshness_check

