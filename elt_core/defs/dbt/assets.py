import dagster as dg
from dagster.components import definitions
from dagster_dbt import build_freshness_checks_from_dbt_assets

from elt_core.defs.dbt.factory import dbt_assets_factory
from elt_core.defs.dbt.constants import (
    SNAPSHOT_SELECTOR,
    TIME_PARTITION_SELECTOR,
)


@definitions
def defs(): 
    assets = [
        dbt_assets_factory("dbt_snapshot_models",
                select=SNAPSHOT_SELECTOR,
                partitioned=False),
        dbt_assets_factory("dbt_partitioned_models",
                select=TIME_PARTITION_SELECTOR,
                partitioned=True),
        dbt_assets_factory("dbt_non_partitioned_models",
                exclude=" ".join([SNAPSHOT_SELECTOR, TIME_PARTITION_SELECTOR]),
                partitioned=False)
    ]

    freshness_checks = build_freshness_checks_from_dbt_assets(dbt_assets=assets)
    freshness_sensor = dg.build_sensor_for_freshness_checks(
        freshness_checks=freshness_checks,
        name="dbt_freshness_checks_sensor"
    )
    

    return dg.Definitions(
        assets=assets,
        asset_checks=freshness_checks,
        sensors=[freshness_sensor]
    )