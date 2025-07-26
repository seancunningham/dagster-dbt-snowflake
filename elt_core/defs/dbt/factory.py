import os
import json
from typing import Callable
from functools import cache
from pathlib import Path

import dagster as dg
from dagster_dbt.asset_utils import DBT_DEFAULT_SELECT
from dagster_dbt import (
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    DbtProject,
    dbt_assets
)

from elt_core.defs.dbt.translator import CustomDagsterDbtTranslator
# from elt_core.defs.dbt.resources import dbt



defer_to_prod = os.getenv("TARGET", "").lower() != "prod"
class DbtConfig(dg.Config):
    full_refresh: bool = False
    defer_to_prod: bool = defer_to_prod
    favor_state: bool = False


@cache
def dbt_assets_factory(
    name: str | None,
    dbt: Callable[[],DbtProject],
    partitioned: bool = False,
    select: str = DBT_DEFAULT_SELECT,
    exclude: str | None = None
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
    def assets(context: dg.AssetExecutionContext, dbt: DbtCliResource, config: DbtConfig):

        args = ["build"]

        if config.full_refresh:
            args.append("--full-refresh")
        if config.defer_to_prod:
            defer_dir = str(Path(__file__).joinpath(*[".."]*4, "dbt/artifacts_prod").resolve())
            args.extend((
                "--defer",
                "--state",
                defer_dir
            ))
            if config.favor_state:
                args.append("--favor-state")

        if partitioned:
            time_window = context.partition_time_window
            format = "%Y-%m-%d %H:%M:%S"
            dbt_vars = {
                "min_date": time_window.start.strftime(format),
                "max_date": time_window.end.strftime(format)
            }

            args.extend(("--vars", json.dumps(dbt_vars)))
            
            yield from dbt.cli(args, context=context).stream().with_insights()
        else:
            yield from dbt.cli(args, context=context).stream().with_insights()
    
    return assets
