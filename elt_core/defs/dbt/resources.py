from functools import cache
from pathlib import Path

from dagster import Definitions
from dagster.components import definitions
from dagster_dbt import DbtCliResource, DbtProject


project_dir = Path(__file__).joinpath(*[".."]*4, "dbt/").resolve()

#TODO switch target in dev
@cache
def dbt() -> DbtProject:
    project = DbtProject(
        project_dir=project_dir
    )
    project.prepare_if_dev()
    return project


@definitions
def defs():
    return Definitions(
        resources={"dbt": DbtCliResource(project_dir=dbt())}
    )
