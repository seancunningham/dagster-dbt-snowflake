from dagster.components import definitions
from dagster import Definitions



@definitions
def defs() -> Definitions:
    from pathlib import Path
    from .factory import build_sling_definitions

    config_dir = Path(__file__).joinpath(*[".."], "sling").resolve()

    return build_sling_definitions(config_dir)
