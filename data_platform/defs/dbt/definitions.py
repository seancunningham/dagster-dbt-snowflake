import os
from pathlib import Path

from dagster import Definitions
from dagster.components import definitions
from dagster_dbt import DbtProject

from .factory import DagsterDbtFactory


@definitions
def defs() -> Definitions:
    """Returns set of definitions explicitly available and loadable by Dagster tools.
    Will be automatically dectectd and loaded by the load_defs function in the root
    definitions file.

    @definitions decorator will provides lazy loading so that the assets are only
    instantiated when needed.
    """
    project_dir = Path(__file__).joinpath(*[".."] * 4, "dbt/").resolve()
    state_path = "state/"


    # .\.venv\Lib\site-packages\dagster_dbt\asset_utils.py
    # commented out 762 as this broke with dbt fusion update preview 9 when child map is
    # not working as dagster expets.
    def dbt() -> DbtProject:
        project = DbtProject(
            project_dir=project_dir,
            target=os.getenv("TARGET", "dev"),
            state_path=state_path,
            profile="dbt",
        )
        if os.getenv("TARGET") == "dev":
            project.prepare_if_dev()
        return project

    return DagsterDbtFactory.build_definitions(dbt)
