"""Minimal stub that emulates the subset of Azure Key Vault functionality used."""

import os
from pathlib import Path


class SecretClient:
    """A stub keyvault to simulate an integration with Azure Keyvault. This would be
    replaced by a keyvault library.
    """

    def get_secret(self, secret_name: str) -> str:
        """Return a secret by name from the in-memory store.

        Args:
            secret_name: Compound key following ``LOCATION__ATTRIBUTE`` naming.

        Returns:
            str: The stored secret value or an empty string when the secret is missing.
        """
        secrets = self.__secrets
        location, _, attribute = secret_name.split("__")
        secret = secrets.get(location, {}).get(attribute)

        return secret or ""

    def __init__(
        self, vault_url: str | None = None, credential: str | None = None
    ) -> None:
        secrets = {"SOURCE": {}, "DESTINATION": {}}

        env = os.getenv("TARGET", "dev")
        env_file = ".env.dev"
        if env == "prod":
            env_file = ".env"
        
        env_path = Path(__file__).joinpath(*[".."] * 3, env_file).resolve()
        with open(env_path) as env:
            for line in env:
                line = line.strip()
                if line:
                    key, value = line.split("=")
                    keys = key.split("__")
                    if len(keys) == 2:
                        location, attribute = keys
                        secrets[location][attribute] = value

        self.__secrets = secrets
