import dlt

from .resources import get_exchange_rate
from ...factory import (
    dlt_assets_factory, add_configs, dlt_freshness_checks_factory
)



default_meta = meta={
    "dagster": {
        "automation_condition": "on_cron_no_deps",
        "automation_condition_config": {"cron_schedule":"@daily", "cron_timezone":"utc"},
        "freshness_lower_bound_delta_seconds": 108000
    }
}

resources = [
    add_configs(
        dlt.resource(
            get_exchange_rate("cad"),
            name="exchange_rate.raw.cad",
            table_name="cad",
            primary_key="date",
            write_disposition="merge"
        ),
        meta=default_meta
    ),

    add_configs(
        dlt.resource(
            get_exchange_rate("usd"),
            name="exchange_rate.raw.usd",
            table_name="usd",
            primary_key="date",
            write_disposition="merge"
        ),
        meta=default_meta
    )
]

kinds = ["api"]
schema = "exchange_rate"
assets, deps = dlt_assets_factory(resources, schema, kinds)
asset_checks = dlt_freshness_checks_factory([assets])