import os
from dagster import Definitions
from dagster.components import definitions
from dagster_dbt import DbtProject


@definitions
def defs() -> Definitions:
    from pathlib import Path
    from .factory import build_dbt_definitions

    project_dir = Path(__file__).joinpath(*[".."]*4, "dbt/").resolve()
    state_path = "state/"

    def dbt() -> DbtProject:
        project = DbtProject(
            project_dir=project_dir,
            target=os.getenv("TARGET", "prod"),
            state_path=state_path,
            profile="dbt"
        )
        if os.getenv("PREPARE_IF_DEV") == "1":
            project.prepare_if_dev()
        return project
    
    return build_dbt_definitions(dbt)