"""Convenience wrappers for retrieving secrets via the stubbed key vault client."""

import os

import dagster as dg

from .keyvault_stub import SecretClient

keyvault = SecretClient(
    vault_url=dg.EnvVar("AZURE_KEYVAULT_URL"),
    credential=dg.EnvVar("AZURE_KEYVAULT_CREDENTIAL"),
)


def get_secret(env_var_name: str) -> dg.EnvVar:
    """Retrieve a secret and expose it as a Dagster ``EnvVar`` reference.

    Args:
        env_var_name: Key identifying the secret in the key vault stub.

    Returns:
        dagster.EnvVar: Environment variable wrapper that defers access to the stored
        secret, ensuring downstream code can request the value securely.
    """
    if secret := keyvault.get_secret(env_var_name):
        os.environ[env_var_name] = secret
        return dg.EnvVar(env_var_name)

    raise ValueError(
        f"Secret for key '{env_var_name}' not found."
        "Please check that this is the correct key."
    )
