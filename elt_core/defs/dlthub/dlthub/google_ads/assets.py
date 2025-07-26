import dlt

from .resources import get_campaigns, get_criterion
from ...factory import (
    dlt_assets_factory,add_configs, dlt_freshness_checks_factory
)



default_meta = {
    "dagster": {
        "automation_condition": "on_cron_no_deps",
        "automation_condition_config": {"cron_schedule":"@daily", "cron_timezone":"utc"},
        "freshness_lower_bound_delta_seconds": 108000
    }
}

resources = [
    add_configs(
        dlt.resource(
            get_campaigns,
            name="google_ads.raw.campaigns",
            table_name="campaigns",
            primary_key="id"
        ),
        meta=default_meta
    ),
    
    add_configs(
        dlt.resource(
            get_criterion,
            name="google_ads.raw.criterion",
            table_name="criterion",
            primary_key="id",
        ),
        meta=default_meta
    )
]

schema = "google_ads"
kinds = ["api"]
assets, deps = dlt_assets_factory(resources, schema, kinds)
asset_checks = dlt_freshness_checks_factory([assets])