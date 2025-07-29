import warnings
from pathlib import Path

import dagster as dg
from dagster.components import definitions

warnings.filterwarnings("ignore", category=dg.BetaWarning)


@definitions
def dbt() -> dg.Definitions:
    project_root = Path(__file__).joinpath(*[".."] * 2).resolve()
    return dg.load_from_defs_folder(project_root=project_root)
