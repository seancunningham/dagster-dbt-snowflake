import os
import yaml
from pathlib import Path
from functools import cache
from typing import Any
from datetime import timedelta

import dagster as dg
from dagster_sling import SlingResource, sling_assets, SlingConnectionResource

from elt_core.utils.secrets import get_secret
from elt_core.defs.sling.translator import CustomDagsterSlingTranslator
from elt_core.utils.transaltor_helpers import sanitize_input_signature



@cache
def sling_factory(config_dir: Path) -> tuple[list[SlingConnectionResource], list[dg.AssetsDefinition], list[dg.AssetChecksDefinition]]:
    connections = []
    assets = []
    asset_checks = []

    for config_path in os.listdir(config_dir):
        if config_path.endswith(".yaml") or config_path.endswith(".yml"):
            config_path = config_dir.joinpath(config_path).resolve()
            with open(config_path, "r") as file:
                config = yaml.load(file, Loader=yaml.FullLoader)
            if not config:
                continue
            

            kind_map = {}
            if connection_configs := config.get("connections"):
                for connection_config in connection_configs:
                    if connection := _get_connection(connection_config):
                        source = connection_config.get("name")
                        kind = connection_config.get("type")
                        kind_map[source] = kind
                        connections.append(connection)


            if replication_configs := config.get("replications"):
                for replication_config in replication_configs:
                    if bool(os.getenv("DAGSTER_IS_DEV_CLI")):
                        replication_config = _set_dev_scheama(replication_config)
                    assets_definition = _get_sling_assets(replication_config)

                    kind = kind_map.get(replication_config.get("source", None), None)
                    dep_asset_specs = _get_sling_deps(replication_config, kind)
                    asset_freshness_checks = _get_freshness_checks(replication_config)
                    
                    if asset_freshness_checks:
                        asset_checks.extend(asset_freshness_checks)
                    if assets_definition:
                        assets.append(assets_definition)
                    if dep_asset_specs:
                        assets.extend(dep_asset_specs)


    return connections, assets, asset_checks

def _get_connection(connection_config: dict) -> SlingConnectionResource | None:
    """parse connection from a replication config"""
    for k, v in connection_config.items():
        if isinstance(v, dict):
            secret_name = list(v.keys())[0]
            display_type = list(v.values())[0]
            
            if display_type == "show":
                connection_config[k] = get_secret(secret_name).get_value()
            else:
                connection_config[k] = get_secret(secret_name)

    connection = SlingConnectionResource(**connection_config)
    return connection


def _get_sling_assets(config: dict) -> dg.AssetsDefinition:
    """Parse assets including dependancies from the replication config"""

    @sling_assets(
            name=config["source"]+"_assets",
            replication_config=config,
            backfill_policy=dg.BackfillPolicy.single_run(),
            dagster_sling_translator=CustomDagsterSlingTranslator()
    )
    def assets(context: dg.AssetExecutionContext, sling: SlingResource):# -> Generator[SlingEventType, Any, None]:
        if "defaults" not in config:
            config["defaults"] = {}
            
        try: # to inject start and end dates for partitioned runs
            time_window = context.partition_time_window
            if time_window:

                if "source_options" not in config["defaults"]:
                    config["defaults"]["source_options"] = {}

                format = "%Y-%m-%d %H:%M:%S"
                start = time_window.start.strftime(format)
                end = time_window.end.strftime(format)
                config["defaults"]["source_options"]["range"] = f"{start},{end}"
        except Exception: # run normal run if time window not provided
            pass

        yield from sling.replicate(context=context,
                                   replication_config=config,
                                   dagster_sling_translator=CustomDagsterSlingTranslator())
        for row in sling.stream_raw_logs():
            context.log.info(row)
    
    return assets


def _set_dev_scheama(replication_config: dict) -> dict:
    user = os.environ["DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME"].upper()
    if default_object := replication_config["defaults"]["object"]:
        schema, table = default_object.split(".")
        replication_config["defaults"]["object"] = f"{schema}__{user}.{table}"

    for stream, stream_config in list(replication_config.get("streams", {}).items()):
        if stream_config:
            if stream_object := stream_config.get("object"):
                schema, table = stream_object.split(".")
                replication_config["streams"][stream]["object"] = f"{schema}__{user}.{table}"

    return replication_config

def _get_sling_deps(replication_config: dict, kind: str | None) -> list[dg.AssetSpec] | None:
    if kind:
        kinds = {kind}
    else:
        kinds = None

    deps = []
    for k in replication_config["streams"].keys():
        schema, table = k.split(".")
        dep = dg.AssetSpec(key=[schema, "src", table], group_name=schema, kinds=kinds)
        deps.append(dep)
    return deps

def _get_nested(config: dict, path: list) -> Any:
    try:
        for item in path:
            config = config[item]
        return config
    except Exception: ...
    return None

def _get_freshness_checks(replication_config: dict) -> list[dg.AssetChecksDefinition]:
    
    freshness_checks = []

    default_freshness_check_config = _get_nested(replication_config, ["defaults", "meta", "dagster", "freshness_check"]) or {}
    default_partition = _get_nested(replication_config, ["defaults", "meta", "dagster", "partition"])

    streams = replication_config.get("streams", {})
    for stream_name, steam_config in streams.items():
        freshness_check_config = _get_nested(steam_config, ["meta", "dagster", "freshness_check"]) or {}
        partition = _get_nested(steam_config, ["meta","dagster","partition"])

        freshness_check_config = freshness_check_config | default_freshness_check_config
        partition = partition or default_partition

        if freshness_check_config:
            if lower_bound_delta_seconds := freshness_check_config.pop("lower_bound_delta_seconds", None):
                lower_bound_delta = timedelta(seconds=float(lower_bound_delta_seconds))
                freshness_check_config["lower_bound_delta"] = lower_bound_delta
            
            schema, table_name = stream_name.split(".")
            asset_key = [schema, "raw", table_name]
            freshness_check_config["assets"] = [asset_key]

            try:
                if partition in ["hourly", "daily", "weekly", "monthly"]:
                    freshness_check_config = sanitize_input_signature(
                        dg.build_time_partition_freshness_checks,
                        freshness_check_config)
                    
                    time_partition_update_freshness_checks = dg.build_time_partition_freshness_checks(
                        **freshness_check_config)
                    freshness_checks.extend(time_partition_update_freshness_checks)

                else:
                    freshness_check_config = sanitize_input_signature(
                        dg.build_last_update_freshness_checks,
                        freshness_check_config)
                    
                    last_update_freshness_checks = dg.build_last_update_freshness_checks(
                        **freshness_check_config)
                    freshness_checks.extend(last_update_freshness_checks)
            except TypeError as e:
                raise TypeError(f"Error creating freshness check, check your configuration for '{asset_key}'. Supplied arguments: {freshness_check_config}")

    return freshness_checks