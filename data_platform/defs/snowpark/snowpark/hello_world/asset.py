"""Helper functions for the sample Snowpark asset."""

from typing import Any

import dagster as dg
from snowflake.snowpark.session import Session


def materialize(
    session: Session,
    context: dg.AssetExecutionContext,
) -> dict[str, Any]:
    """Log a trivial message and fetch data to demonstrate Snowpark usage.

    Args:
        session: Active Snowpark session injected by the Dagster resource.
        context: Dagster execution context used for structured logging.

    Returns:
        dict[str, Any]: Placeholder dictionary for parity with Dagster Snowpark asset
        expectations. The example does not materialize data back to Snowflake.
    """

    context.log.info("hello world")
    session.table("transactions")

    return {}
