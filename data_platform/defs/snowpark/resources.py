"""Snowpark resource definitions that manage sessions for Dagster assets."""

import os
import sys

import dagster as dg
from dagster.components import definitions
from snowflake.snowpark import Session

from ...utils.helpers import get_database_name, get_schema_name


class SnowparkResource(dg.ConfigurableResource):
    """Resource class for managing Snowpark sessions."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._session = None

    def get_session(
        self,
        database: str = "analytics",
        schema: str | None = None,
        warehouse: str | None = None,
    ) -> Session:
        """Create or reuse a Snowpark session with environment aware defaults.

        Args:
            database: Logical database to connect to; defaults to ``analytics``.
            schema: Optional schema override; defaults to the destination user when
                omitted.
            warehouse: Optional Snowflake warehouse; defaults to the environment
                variable ``DESTINATION__WAREHOUSE`` when not provided.

        Returns:
            snowflake.snowpark.Session: The active Snowpark session configured using the
            resolved parameters.
        """

        if sys.platform == "win32":
            import pathlib
            pathlib.PosixPath = pathlib.PurePosixPath

        if schema:
            schema = get_schema_name(schema)
        else:
            # Default to the configured destination user which is namespaced per env.
            schema = os.getenv("DESTINATION__USER", "")


        if not warehouse:
            # Use the warehouse provided via environment when not explicitly overridden.
            warehouse = os.getenv("DESTINATION__WAREHOUSE", "")
        self._session = (
            Session.builder.configs({
                "database":  get_database_name(database),
                "account":   os.getenv("DESTINATION__HOST", ""),
                "user":      os.getenv("DESTINATION__USER", ""),
                "password":  os.getenv("DESTINATION__PASSWORD", ""),
                "role":      os.getenv("DESTINATION__ROLE", ""),
                "warehouse": os.getenv("DESTINATION__WAREHOUSE", ""),
            })
            .create()
        )

        try:
            self._session.use_schema(schema)
        except Exception:
            self._session.sql(f"create schema if not exists {schema}")
            self._session.use_schema(schema)

        return self._session


@definitions
def defs() -> dg.Definitions:
    """Expose the Snowpark resource so Snowflake backed assets can access it.

    Returns:
        dagster.Definitions: Definitions object containing a ``snowpark`` resource
        instance for downstream assets.
    """
    return dg.Definitions(resources={"snowpark": SnowparkResource()})
