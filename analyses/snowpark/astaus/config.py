import os
from pathlib import Path

from dotenv import load_dotenv
from snowflake.snowpark import Session


def get_snowpark_session() -> Session:
    path = Path(__file__).joinpath("../../../../.env.dev").resolve()
    load_dotenv(path)

    session = Session.get_active_session()
    if session:
        session.close()

    def _var(key) -> str:
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