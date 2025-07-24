from typing import Any
from datetime import datetime

import dagster as dg

from elt_core.defs.automation_conditions import CustomAutomationCondition


def get_automation_condition_from_meta(meta: dict[str: Any]) -> dg.AutomationCondition | None:
    try:
        condition_name = meta.get("automation_condition")
        condition = CustomAutomationCondition.get_automation_condition(condition_name)
        if condition:
            condition_config = meta.get("automation_condition_config")
            args, kwargs = [], {}
            if condition_config:
                if isinstance(condition_config, list):
                    args = condition_config
                if isinstance(condition_config, dict):
                    kwargs = condition_config
            return condition(*args, **kwargs)
    except Exception: ...
    return None

def get_partitions_def_from_meta(meta: dict[str: Any]) -> dg.TimeWindowPartitionsDefinition:
    try:
        partition = meta.get("partition")
        partition_start_date = meta.get("partition_start_date")
        
        if partition:
            if partition_start_date:
                start_date = datetime.fromisoformat(partition_start_date)
                if partition == "hourly":
                        return dg.HourlyPartitionsDefinition(start_date=start_date.strftime("%Y-%m-%d-%H:%M"))
                if partition == "daily":
                        return dg.DailyPartitionsDefinition(start_date=start_date.strftime("%Y-%m-%d"))
                if partition == "weekly":
                        return dg.WeeklyPartitionsDefinition(start_date=start_date.strftime("%Y-%m-%d"))
                if partition == "monthly":
                        return dg.MonthlyPartitionsDefinition(start_date=start_date.strftime("%Y-%m-%d"))
    except Exception: ...
    return None