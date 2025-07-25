import dagster as dg
from dagster.components import definitions


@definitions
def defs() -> dg.Definitions:
    return dg.Definitions(
        sensors=[
            dg.AutomationConditionSensorDefinition(
                name="automation_condition_sensor",
                target=dg.AssetSelection.all()
            )
        ]
    )