import os

from elt_core.utils.secrets import SecretClient


# from keyvault
environment_variables = [
    "DESTINATION__SNOWFLAKE__CREDENTIALS__HOST",
    "DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME",
    "DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD",
    "DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE",
    "DESTINATION__SNOWFLAKE__CREDENTIALS__ROLE",
    "DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE",
]

azure_keyvault = SecretClient(
    vault_url=os.getenv("AZURE_KEYVAULT_URL"),
    credential=os.getenv("AZURE_KEYVAULT_CREDENTIAL")
)

try:
    for var in environment_variables:
        if not os.getenv(var):
            os.environ[var] = azure_keyvault.get_secret(var)

    os.environ["ENABLE_DATASET_NAME_NORMALIZATION"] = "false"
except UnboundLocalError:
    dev_state = bool(os.getenv("DAGSTER_IS_DEV_CLI"))
    raise UnboundLocalError(f"Secret not set: '{var}'. Dev state: `{dev_state}`")