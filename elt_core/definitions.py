import warnings

import dagster as dg
from dagster.components import definitions

warnings.filterwarnings("ignore", category=dg.BetaWarning)


@definitions
def dbt() -> dg.Definitions:
    import elt_core.defs
    return dg.load_defs(elt_core.defs)



    # sensors = list(defs.sensors or [])
    # automation_condition_sensor = dg.AutomationConditionSensorDefinition(
    #     name="sensor_for_automation_conditions",
    #     target=dg.AssetSelection.all()
    # )
    # sensors.append(automation_condition_sensor)

    # dg.Definitions(
    #     assets=defs.assets,
    #     schedules=defs.schedules,
    #     sensors=sensors,
    #     jobs=defs.jobs,
    #     resources=defs.resources,
    #     executor=defs.executor,
    #     loggers=defs.loggers,
    #     asset_checks=defs.asset_checks,
    #     metadata=defs.metadata,
    #     component_tree=defs.component_tree
    # )

    # return dg.load_defs(elt_core.defs)
