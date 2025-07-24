import re
from collections.abc import Mapping
from typing import Any, Optional

import dagster as dg
from dagster_dbt import DagsterDbtTranslator

from elt_core.defs.automation_conditions import CustomAutomationCondition
from elt_core.utils.transaltor_helpers import (
     get_automation_condition_from_meta,
     get_partitions_def_from_meta
)

dg.AssetCheckSpec


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> dg.AssetKey:
        meta = dbt_resource_props.get("config", {}).get("meta", {}) or dbt_resource_props.get(
            "meta", {}
        )
        asset_key_config = meta.get("asset_key", [])
        if asset_key_config:
            return dg.AssetKey(asset_key_config)

        prop_key = "name"
        if dbt_resource_props.get("version"):
            prop_key = "alias"

        if dbt_resource_props["resource_type"] == "source":
            schema = dbt_resource_props["source_name"]
            table = dbt_resource_props["name"]
            step = "raw"
            return dg.AssetKey([schema, step, table])

        parsed_name = re.search("(.*?)_(.*)__(.*)", dbt_resource_props[prop_key])
        if parsed_name:
            schema = parsed_name.group(2)
            table = parsed_name.group(3)
            step = parsed_name.group(1)
            return dg.AssetKey([schema, step, table])

        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str:
        prop_key = "name"
        if dbt_resource_props.get("version"):
            prop_key = "alias"
        parsed_name = re.search("(.*?)_(.*)__(.*)", dbt_resource_props[prop_key])
        if parsed_name:
            schema = parsed_name.group(2)
            return schema

        return super().get_group_name(dbt_resource_props)

    def get_partitions_def(self, dbt_resource_props: Mapping[str, Any]) -> Optional[dg.PartitionsDefinition]:
        meta = dbt_resource_props.get("config").get("meta", {}).get("dagster", {})
        return get_partitions_def_from_meta(meta)

    def get_automation_condition(self, dbt_resource_props: Mapping[str, Any]) -> Optional[dg.AutomationCondition]:
        meta = dbt_resource_props.get("config").get("meta", {}).get("dagster", {})
        automation_condition = get_automation_condition_from_meta(meta)
        if automation_condition:
             return automation_condition

        # default settings for resource types
        resource_type = dbt_resource_props.get("resource_type")
        if resource_type == "snapshot":
            return CustomAutomationCondition.eager_with_deps_checks()

        if resource_type == "seed":
             return CustomAutomationCondition.code_version_changed()

        if resource_type == "model":
             return CustomAutomationCondition.lazy()
        

    def get_tags(self, dbt_resource_props: Mapping[str, Any]):
        # meta = dbt_resource_props.get("config").get("meta", {}).get("dagster", {})
        tags = super().get_tags(dbt_resource_props)
        # freshness_lower_bound_delta = meta.get("freshness_lower_bound_delta")
        # if freshness_lower_bound_delta:
        #     tags["freshness_lower_bound_delta"] = str(freshness_lower_bound_delta)
        return tags