import dlt

from elt_core.defs.dlthub.factory import dlt_assets_factory, add_configs, dlt_freshness_checks_factory
from .resources import get_campaigns, get_criterion


schema = "google_ads"
kinds = ["google"]

resources = [
    add_configs(
        dlt.resource(
            get_campaigns,
            name="google_ads.raw.campaigns",
            table_name="campaigns",
            primary_key="id"
        ),
        meta={
            "dagster": {
                "automation_condition": "on_cron_no_deps",
                "automation_condition_config": {"cron_schedule":"@daily", "cron_timezone":"utc"},
                "freshness_lower_bound_delta": 1800
            }
        }
    ),
    
    add_configs(
        dlt.resource(
            get_criterion,
            name="google_ads.raw.criterion",
            table_name="criterion",
            primary_key="id",
        ),
        meta={
            "dagster": {
                "automation_condition": "on_cron_no_deps",
                "automation_condition_config": {"cron_schedule":"@daily", "cron_timezone":"utc"},
                "freshness_lower_bound_delta": 1800
            }
        }
    )
]


assets, deps = dlt_assets_factory(resources, schema, kinds)
asset_checks = dlt_freshness_checks_factory([assets])