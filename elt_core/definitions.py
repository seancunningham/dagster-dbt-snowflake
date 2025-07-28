import warnings

import dagster as dg
from dagster.components import definitions

warnings.filterwarnings("ignore", category=dg.BetaWarning)


@definitions
def dbt() -> dg.Definitions:
    import elt_core.defs
    return dg.load_defs(elt_core.defs)
