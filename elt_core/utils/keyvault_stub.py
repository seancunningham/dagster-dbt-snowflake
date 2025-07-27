import os

class SecretClient():
    """A stub keyvault to simulate an integration with Azure Keyvault.
    This would be replaced by a keyvault library.
    """

    def get_secret(self, secret_name:str) -> str:
        """returns a secret from the keyvault
        """
        
        secrets = self.__secrets
        secret = None
        if os.getenv("TARGET") == "dev":
            secrets = self.__dev_secrets
            secret = os.getenv(secret_name)
        
        try:
            if not secret:
                location, _, config_item = secret_name.split("__")
                secret = secrets.get(location, {}).get(config_item)
        except Exception: ...
        
        if not secret:
            secret = ""
        return secret

    def __init__(self, vault_url:str|None=None, credential:str|None=None) -> None:

        self.__secrets = { 
            "SOURCE":{
                "DATABASE":os.getenv("SOURCE_DATABASE"),
                "HOST":os.getenv("SOURCE_HOST"),
                "PORT":os.getenv("SOURCE_PORT"),
                "USER":os.getenv("SOURCE_USER"),
                "PASSWORD":os.getenv("SOURCE_PASSWORD")
            },

            "DESTINATION":{
                "HOST":os.getenv("DESTINATION__SNOWFLAKE__HOST"),
                "DATABASE":os.getenv("DESTINATION__SNOWFLAKE__DATABASE"),
                "PASSWORD":os.getenv("DESTINATION__SNOWFLAKE__PASSWORD"),
                "USER":os.getenv("DESTINATION__SNOWFLAKE__USER"),
                "ROLE":os.getenv("DESTINATION__SNOWFLAKE__ROLE"),
                "WAREHOUSE":os.getenv("DESTINATION__SNOWFLAKE__WAREHOUSE"),
            }
        }
        
        self.__dev_secrets = {
            "SOURCE": {
                "DATABASE":os.getenv("SOURCE_DATABASE"),
                "HOST":os.getenv("SOURCE_HOST"),
                "PORT":os.getenv("SOURCE_PORT"),
                "USER":os.getenv("SOURCE_USER"),
                "PASSWORD":os.getenv("SOURCE_PASSWORD")
            }
        }