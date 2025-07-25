import os

import dagster as dg
from dagster.components import definitions
from dagster_dlt import DagsterDltResource

from elt_core.key_vault import SecretClient


@definitions
def defs() -> dg.Definitions:

    kv = SecretClient(
        vault_url=os.getenv("AZURE_KEYVAULT_URL"),
        credential=os.getenv("AZURE_KEYVAULT_CREDENTIAL")
    )

    os.environ["DESTINATION__SNOWFLAKE__CREDENTIALS__HOST"] = (
        kv.get_secret("DESTINATION__SNOWFLAKE__HOST")
    )
    os.environ["DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME"] = (
        kv.get_secret("DESTINATION__SNOWFLAKE__USER")
    )
    os.environ["DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD"] = (
        kv.get_secret("DESTINATION__SNOWFLAKE__PASSWORD")
    )
    os.environ["DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE"] = (
        kv.get_secret("DESTINATION__SNOWFLAKE__DATABASE")
    )
    os.environ["DESTINATION__SNOWFLAKE__CREDENTIALS__ROLE"] = (
        kv.get_secret("DESTINATION__SNOWFLAKE__ROLE")
    )
    os.environ["DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE"] = (
        kv.get_secret("DESTINATION__SNOWFLAKE__WAREHOUSE")
    )

    os.environ["ENABLE_DATASET_NAME_NORMALIZATION"] = "false"

    return dg.Definitions(
        resources={
            "dlt": DagsterDltResource()
        },
    )