import dagster as dg
from dagster.components import definitions
import dlt
from ...factory import (
    build_dlt_definitions, config
)
@definitions
def defs() -> dg.Definitions:
#######################################################
    from .resources import get_campaigns
    resources = [
#######################################################
        config(dlt.resource(
                get_campaigns,
                name="facebook_ads.campaigns",
                table_name="campaigns",
                primary_key="id",
                write_disposition="merge"
            ),
            kinds={"api"},
            meta={
                "dagster": {
                    "automation_condition": "on_cron_no_deps",
                    "automation_condition_config": {
                        "cron_schedule":"@daily",
                        "cron_timezone":"utc"},
                    "freshness_lower_bound_delta_seconds": 108000
                }
            }
        )
#######################################################
    ]
    return build_dlt_definitions(tuple(resources))

