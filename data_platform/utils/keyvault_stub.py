import os
from pathlib import Path


class SecretClient:
    """A stub keyvault to simulate an integration with Azure Keyvault. This would be
    replaced by a keyvault library.
    """

    def get_secret(self, secret_name: str) -> str:
        """returns a secret from the keyvault"""
        secrets = self.__secrets
        location, _, attribute = secret_name.split("__")
        secret = secrets.get(location, {}).get(attribute)

        return secret or ""

    def __init__(
        self, vault_url: str | None = None, credential: str | None = None
    ) -> None:
        secrets = {"SOURCE": {}, "DESTINATION": {}}

        env_path = Path(__file__).joinpath(*[".."] * 3, ".env").resolve()
        set_env = os.getenv("TARGET", "")
        with open(env_path) as env:
            for line in env:
                line = line.strip()
                if line.startswith(set_env.upper()) or line.startswith("ANY"):
                    key, secret = line.split("=")
                    env, location, attribute = key.split("__")
                    secrets[location][attribute] = secret

        self.__secrets = secrets
