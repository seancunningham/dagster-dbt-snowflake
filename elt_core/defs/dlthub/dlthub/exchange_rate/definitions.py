import dlt
import dagster as dg
from dagster.components import definitions
from ...factory import build_dlt_definitions, config


#######################################################
@definitions
def defs() -> dg.Definitions:
    from .resources import get_exchange_rate
    resources = [
#######################################################
        config(dlt.resource(
                get_exchange_rate("cad"),
                name="exchange_rate.cad",
                table_name="cad",
                primary_key="date",
                write_disposition="merge"
            ),
            kinds={"api"},
            meta={
                "dagster": {
                    "automation_condition": "on_cron_no_deps",
                    "automation_condition_config": {
                        "cron_schedule":"@daily",
                        "cron_timezone":"utc"},
                    # "freshness_lower_bound_delta_seconds": 108000
                }
            }
        ),
#######################################################
        config(dlt.resource(
                get_exchange_rate("usd"),
                name="exchange_rate.usd",
                table_name="usd",
                primary_key="date",
                write_disposition="merge"
            ),
            kinds={"api"},
            meta={
                "dagster": {
                    "automation_condition": "on_cron_no_deps",
                    "automation_condition_config": {
                        "cron_schedule":"@daily",
                        "cron_timezone":"utc"},
                    # "freshness_lower_bound_delta_seconds": 108000
                }
            }
        )
#######################################################
    ]
    return build_dlt_definitions(tuple(resources))

