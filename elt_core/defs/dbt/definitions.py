import os
from functools import cache
from pathlib import Path

import dagster as dg
from dagster.components import definitions
from dagster_dbt import (
    DbtCliResource,
    DbtProject,
    build_freshness_checks_from_dbt_assets
)

from elt_core.defs.dbt.factory import dbt_assets_factory
from elt_core.defs.dbt.constants import TIME_PARTITION_SELECTOR


@definitions
def defs() -> dg.Definitions:
    project_dir = Path(__file__).joinpath(*[".."]*4, "dbt/").resolve()
    state_path = "state/"

    @cache
    def dbt() -> DbtProject:
        project = DbtProject(
            project_dir=project_dir,
            target=os.getenv("TARGET", "prod"),
            state_path=state_path,
            profile="dbt"
        )
        project.prepare_if_dev()
        return project
    
    assets = [
        dbt_assets_factory("dbt_partitioned_models",
                dbt=dbt,
                select=TIME_PARTITION_SELECTOR,
                partitioned=True),
        dbt_assets_factory("dbt_non_partitioned_models",
                dbt=dbt,
                exclude=TIME_PARTITION_SELECTOR,
                partitioned=False)
    ]

    freshness_checks = build_freshness_checks_from_dbt_assets(dbt_assets=assets)
    freshness_sensor = dg.build_sensor_for_freshness_checks(
        freshness_checks=freshness_checks,
        name="dbt_freshness_checks_sensor"
    )

    return dg.Definitions(
        resources={
            "dbt": DbtCliResource(project_dir=dbt())
        },
        assets=assets,
        asset_checks=freshness_checks,
        sensors=[freshness_sensor]
    )
