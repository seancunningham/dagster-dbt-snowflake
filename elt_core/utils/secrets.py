import os

import dagster as dg
from ..key_vault import SecretClient


azure_keyvault = SecretClient(
    vault_url=dg.EnvVar("AZURE_KEYVAULT_URL"),
    credential=dg.EnvVar("AZURE_KEYVAULT_CREDENTIAL")
)

def get_secret(env_var_name: str) -> dg.EnvVar:
    """Get a secret from the keyvault and set it to an environment variable
    that can be used securly with dagsters EnvVar class."""
    try:
        os.environ[env_var_name] = azure_keyvault.get_secret(env_var_name)
    except TypeError:
        raise ValueError(f"Secret for key '{env_var_name}' not found. Please check that this is the correct key.")
    
    return dg.EnvVar(env_var_name)