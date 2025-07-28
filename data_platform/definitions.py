import warnings

import dagster as dg
from dagster.components import definitions

warnings.filterwarnings("ignore", category=dg.BetaWarning)


@definitions
def dbt() -> dg.Definitions:
    import data_platform.defs
    return dg.load_defs(data_platform.defs)
