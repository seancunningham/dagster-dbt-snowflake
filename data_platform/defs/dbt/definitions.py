"""Dagster definition entry point for orchestrating the example dbt project.

This module wires the dbt project metadata into the :class:`DagsterDbtFactory`
so that the resulting :class:`dagster.Definitions` object exposes assets, sensors,
and resources consistent with a real deployment.
"""

from pathlib import Path

from dagster import Definitions
from dagster.components import definitions
from dagster_dbt import DbtProject

from ...config import get_current_environment
from .factory import DagsterDbtFactory


@definitions
def defs() -> Definitions:
    """Construct Dagster definitions for the configured dbt project.

    Returns:
        dagster.Definitions: A factory-produced ``Definitions`` object containing the
        dbt asset groups, shared resources, and freshness monitoring sensors derived
        from the on-disk dbt project.
    """
    project_dir = Path(__file__).joinpath(*[".."] * 4, "dbt/").resolve()
    env = get_current_environment()
    state_path = env.state_path("state/")


    # .\.venv\Lib\site-packages\dagster_dbt\asset_utils.py
    # added: if unique_id in child_map.keys():
    # on line 816.  As of dbt fusion 2.09 the child map
    # does not poulate keys for assets if the asset has no children
    # resulting in key error when loading definitions.
    def dbt() -> DbtProject:
        """Instantiate a :class:`DbtProject` with environment-aware configuration.

        Returns:
            dagster_dbt.DbtProject: The fully configured dbt project instance that
            Dagster will interact with when executing assets. The helper runs
            ``prepare_if_dev`` to ensure the project is ready for local execution when
            targeting a development profile.
        """
        project = DbtProject(
            project_dir=project_dir,
            target=env.dbt_target,
            state_path=state_path,
            profile="dbt",
        )
        # if os.getenv("TARGET") == "dev":
        project.prepare_if_dev()
        return project

    return DagsterDbtFactory.build_definitions(dbt)
