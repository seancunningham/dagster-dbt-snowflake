"""Helper utilities for creating Snowpark sessions during local exploration.

This module loads credentials from the project's ``.env.dev`` file and exposes a
single convenience function, :func:`get_snowpark_session`, that eagerly closes any
existing Snowpark session before returning a fresh connection configured for the
development database.
"""

import os
from pathlib import Path

from dotenv import load_dotenv
from snowflake.snowpark import Session


def get_snowpark_session() -> Session:
    """Create and return a development-scoped Snowpark session.

    Returns:
        Session: A newly created Snowpark session authenticated with credentials
        sourced from ``.env.dev``. Any previously active session is closed before
        the new session is established so callers always interact with a single
        live connection.
    """
    path = Path(__file__).joinpath("../../../../.env.dev").resolve()
    load_dotenv(path)

    session = Session.get_active_session()
    if session:
        session.close()

    def _var(key: str) -> str:
        """Fetch an environment variable used to parameterize the session.

        Args:
            key: The exact environment variable name to read from the current
                process.

        Returns:
            str: The resolved value or an empty string when the variable is not
            set. Returning an empty string matches Snowpark's expectation that
            optional fields exist but may be blank in local development.
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