"""Utilities tailored for experimenting with Snowflake ML via Snowpark locally.

The helpers here mirror :mod:`analyses.snowpark.astaus.config` but remain isolated so
Snowflake ML notebooks can depend on a minimal module that knows how to hydrate a
session with the expected machine learning role and warehouse.
"""

import os
from pathlib import Path

from dotenv import load_dotenv
from snowflake.snowpark import Session


def get_snowpark_session() -> Session:
    """Provision a Snowpark session suitable for Snowflake ML experimentation.

    Returns:
        Session: A newly initialized Snowpark session that authenticates using
        ``.env.dev`` values. If an existing session is detected it is closed first
        so Snowflake ML operations always run against a clean connection.
    """
    path = Path(__file__).joinpath("../../../../../.env.dev").resolve()
    load_dotenv(path)

    session = Session.get_active_session()
    if session:
        session.close()

    def _var(key: str) -> str:
        """Retrieve a Snowflake credential from the process environment.

        Args:
            key: The fully qualified environment variable name expected by the
                Snowpark session builder.

        Returns:
            str: The resolved environment variable value or an empty string when
            unset. Returning a blank string ensures optional connection fields do
            not raise errors during local ML development.
        """
        return os.getenv(key) or ""

    session = (
        Session.builder.configs({ 
            "database":  "_dev_analytics",
            "schema":    _var("DESTINATION__USER"),
            "account":   _var("DESTINATION__HOST"),
            "user":      _var("DESTINATION__USER"),
            "password":  _var("DESTINATION__PASSWORD"),
            "role":      _var("DESTINATION__ROLE"),
            "warehouse": _var("DESTINATION__WAREHOUSE"),
        }) 
        .create()
    )

    print("session_id:", session.session_id)
    print("version:",    session.version)
    print("database:",   session.get_current_database())
    print("schema:",     session.get_current_schema())
    print("user:",       session.get_current_user())

    return session

if __name__ == "__main__":
    get_snowpark_session()