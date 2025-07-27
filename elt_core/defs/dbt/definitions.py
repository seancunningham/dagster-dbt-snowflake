import os
from dagster import Definitions
from dagster.components import definitions
from dagster_dbt import DbtProject


@definitions
def defs() -> Definitions:
    """Returns set of definitions explicitly available and loadable by Dagster tools.
    Will be automatically dectectd and loaded by the load_defs function in the root
    definitions file.

    @definitions decorator will provides lazy loading so that the assets are only
    instantiated when needed."""

    from pathlib import Path
    from .utils import DagsterDbtFactory

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
    
    return DagsterDbtFactory.build_definitions(dbt)