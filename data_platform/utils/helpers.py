"""Shared helper utilities used across Dagster definition factories."""

"""Shared helper utilities used across Dagster definition factories and resources."""

import os
from collections.abc import Callable, Mapping
from datetime import datetime
from inspect import signature
from typing import Any

import dagster as dg

from .automation_conditions import CustomAutomationCondition


def get_schema_name(schema: str) -> str:
    """Return the schema name adjusted for the current environment.

    Args:
        schema: Base schema name defined in configuration.

    Returns:
        str: Schema name suffixed with the destination user when targeting ``dev`` to
        ensure isolation between developers.
    """
    postfix = os.getenv("DESTINATION__USER", "")
    if os.getenv("TARGET") == "dev":
        schema = f"{schema}__{postfix}"
    return schema

def get_database_name(database: str) -> str:
    """Return the database name adjusted for the current environment.

    Args:
        database: Base database name configured for the deployment.

    Returns:
        str: Database name optionally prefixed with ``_dev_`` in development
        environments.
    """
    if os.getenv("TARGET") == "dev":
        database = f"_dev_{database}"
    return database

def get_automation_condition_from_meta(
    meta: dict[str, Any],
) -> dg.AutomationCondition | None:
    """Return an automation condition if valid configuration is provided in metadata.

    Meta should be of format dict in the following structure:
    .. code-block:: python
        "meta":{
            "dagster":{
                "automation_condition": condition,
                "automation_condition_config": {argument: value}
            }
        }
    """
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
        # e.message(
        #     "'condition_config' is missing required keys"
        #     f"for condition '{condition_name}'"
        # )
        raise e


def get_partitions_def_from_meta(
    meta: dict[str, Any],
) -> dg.TimeWindowPartitionsDefinition | None:
    """Return a partitions definition when valid configuration is provided in metadata.
    - partition accepts the values: hourly, daily, weekly, monthly.
    - partition_start_date should be a iso format date, or timestamp.

    Meta should be of format dict in the following structure:
    .. code-block:: python
       "meta":{
           "dagster":{
               "partition": "daily",
               "partition_start_date": "2025-01-01"
           }
       }
    """
    try:
        partition = meta.get("partition")
        partition_start_date = meta.get("partition_start_date")

        if partition and partition_start_date:
            start_date = datetime.fromisoformat(partition_start_date)
            if partition == "hourly":
                return dg.HourlyPartitionsDefinition(
                    start_date=start_date.strftime("%Y-%m-%d-%H:%M")
                )
            if partition == "daily":
                return dg.DailyPartitionsDefinition(
                    start_date=start_date.strftime("%Y-%m-%d")
                )
            if partition == "weekly":
                return dg.WeeklyPartitionsDefinition(
                    start_date=start_date.strftime("%Y-%m-%d")
                )
            if partition == "monthly":
                return dg.MonthlyPartitionsDefinition(
                    start_date=start_date.strftime("%Y-%m-%d")
                )
    except Exception:
        ...
    return None


def sanitize_input_signature(func: Callable, kwargs: dict) -> dict:
    """Remove any arguments that are not expected by the receiving function.

    Args:
        func: Callable whose signature should be respected.
        kwargs: Proposed keyword arguments to sanitize.

    Returns:
        dict: Filtered keyword arguments containing only parameters accepted by ``func``.
    """
    sig = signature(func)
    key_words = list(kwargs.keys())
    expected_arguments = {argument for argument, _ in sig.parameters.items()}

    for argument in key_words:
        if argument not in expected_arguments:
            kwargs.pop(argument)

    return kwargs


def get_nested(config: Mapping[str, Any], path: list[str]) -> Any:
    """Safely traverse a nested mapping that may include ``None`` placeholders.

    The helper is useful because stream definitions that rely on defaults often contain
    keys with ``null`` values until overridden.
    .. code-block:: yaml
    streams:
        source.table_one:
        source.table_two:
    """
    try:
        for item in path:
            config = config[item]
        return config
    except Exception:
        ...
    return None
