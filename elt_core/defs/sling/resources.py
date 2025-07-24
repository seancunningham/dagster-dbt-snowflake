from pathlib import Path

import dagster as dg
from dagster.components import definitions
from dagster_sling import SlingResource

from .factory import sling_factory



config_dir = Path(__file__).joinpath(*[".."], "configs").resolve()

@definitions
def defs() -> dg.Definitions:
    connections, assets, freshness_checks = sling_factory(config_dir)
    freshness_sensor = dg.build_sensor_for_freshness_checks(
        freshness_checks=freshness_checks,
        name="sling_freshness_checks_sensor"
    )

    return dg.Definitions(
        resources={"sling": SlingResource(connections=connections)},
        assets=assets,
        asset_checks=freshness_checks,
        sensors=[freshness_sensor]
    )
