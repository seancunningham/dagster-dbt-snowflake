import warnings

import dagster as dg

import elt_core.env


warnings.filterwarnings("ignore", category=dg.BetaWarning)

@dg.components.definitions
def defs() -> dg.Definitions:
    import elt_core.defs

    return dg.components.load_defs(elt_core.defs)
