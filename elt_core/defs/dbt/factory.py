import json
from functools import cache

import dagster as dg
from dagster_dbt.asset_utils import DBT_DEFAULT_SELECT
from dagster_dbt import (
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    dbt_assets
)

from elt_core.defs.dbt.translator import CustomDagsterDbtTranslator
from elt_core.defs.dbt.resources import dbt


@cache
def dbt_assets_factory(
    name: str = None,
    partitioned: bool = False,
    select: None | str = DBT_DEFAULT_SELECT,
    exclude: None | str = None
    ) -> dg.AssetsDefinition:
    
    dbt_project = dbt()
    assert dbt_project

    @dbt_assets(
        name=name,
        manifest=dbt_project.manifest_path,
        select=select,
        exclude=exclude,
        dagster_dbt_translator=CustomDagsterDbtTranslator(
            settings=DagsterDbtTranslatorSettings(
                enable_duplicate_source_asset_keys=True,
            )
        ),
        backfill_policy=dg.BackfillPolicy.single_run(),
        project=dbt_project,
    )
    def assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
        if partitioned:
            time_window = context.partition_time_window
            format = "%Y-%m-%d %H:%M:%S"
            dbt_vars = {
                "min_date": time_window.start.strftime(format),
                "max_date": time_window.end.strftime(format)
            }
            yield from dbt.cli(["build", "--vars", json.dumps(dbt_vars)], context=context).stream().with_insights()
        else:
            yield from dbt.cli(["build"], context=context).stream().with_insights()
    
    return assets