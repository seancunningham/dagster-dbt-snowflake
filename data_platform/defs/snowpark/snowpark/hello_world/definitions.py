"""Example Snowpark asset that showcases boilerplate integration with Dagster."""

import dagster as dg

from .....utils.automation_conditions import CustomAutomationCondition
from ...resources import SnowparkResource
from .asset import materialize


@dg.asset(
    group_name="marketing",
    key=["marketing", "snowpark", "hello_world"],
    deps=[["marketing", "fct", "fct_attributions"]],
    kinds={"snowpark"},
    description="Example Snowpark asset to showcase boilerplate",
    automation_condition=CustomAutomationCondition.on_cron("@daily"),
)
def hello_world(
    context: dg.AssetExecutionContext,
    snowpark: SnowparkResource,
) -> dg.MaterializeResult:
    """Materialize a simple Snowpark asset, logging metadata for demo purposes.

    Args:
        context: Dagster execution context for structured logging and metadata.
        snowpark: Configured Snowpark resource supplying authenticated sessions.

    Returns:
        dagster.MaterializeResult: Result object capturing any metadata emitted during
        the Snowpark session execution.
    """

    session = snowpark.get_session(schema="transactions_db")

    result_metadata = materialize(session, context)

    return dg.MaterializeResult(metadata=result_metadata)
