from typing import Any, Callable
from datetime import datetime
from inspect import signature

import dagster as dg

from elt_core.defs.automation_conditions import CustomAutomationCondition



def get_automation_condition_from_meta(meta: dict[str, Any]) -> dg.AutomationCondition | None:

    condition_name = meta.get("automation_condition")
    if not condition_name:
         return None
    
    condition = CustomAutomationCondition.get_automation_condition(condition_name)
    if not isinstance(condition, Callable):
        raise KeyError(f"Automation condition not found for key '{condition_name}'")
    
    condition_config = meta.get("automation_condition_config", {}) or {}
    if not isinstance(condition_config, dict):
            raise ValueError(f"Invalid condition config: '{condition_config}'")

    condition_config = sanitize_input_signature(condition, condition_config)
    try:
        return condition(**condition_config)
    except Exception as e:
         e.add_note(f"'condition_config' is missing required keys for condition '{condition_name}'")
         raise
    

def get_partitions_def_from_meta(meta: dict[str, Any]) -> dg.TimeWindowPartitionsDefinition | None:
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

def sanitize_input_signature(func: Callable, kwargs: dict) -> dict:
    """Remove any arguments that are not expected by the recieving function"""
    sig = signature(func)
    key_words = list(kwargs.keys())
    expected_arguments = {argument for argument, _ in sig.parameters.items()}

    for argument in key_words:
        if argument not in expected_arguments:
            kwargs.pop(argument)
    
    return kwargs