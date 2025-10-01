"""Centralized environment configuration for Dagster/dbt deployments."""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from functools import lru_cache

ENVIRONMENT_SEQUENCE: tuple[str, ...] = ("lab", "dev", "qa", "staging", "prod")


@dataclass(frozen=True)
class EnvironmentConfig:
    """Declarative settings for a single deployment environment."""

    name: str
    dbt_target: str
    database_prefix: str = ""
    schema_suffix_template: str | None = None
    dataset_suffix_template: str | None = None
    state_subdir: str | None = None
    keyvault_env_file: str | None = None

    def database_name(self, base: str) -> str:
        """Return the database name adjusted for this environment.

        Args:
            base: Logical database name defined by the project.

        Returns:
            str: Database name prefixed with ``database_prefix`` when configured.
        """

        if self.database_prefix:
            return f"{self.database_prefix}{base}"
        return base

    def schema_name(self, base: str, user: str | None = None) -> str:
        """Return an environment-scoped schema name.

        Args:
            base: Schema identifier prior to environment adjustments.
            user: Optional username used to render user-specific suffix templates.

        Returns:
            str: Schema name including any configured suffix template rendered with the
            supplied ``user``.
        """

        if not self.schema_suffix_template:
            return base

        if user:
            return f"{base}{self.schema_suffix_template.format(user=user)}"
        return base

    def dataset_name(self, base: str, user: str | None = None) -> str:
        """Return the dataset name used by ingestion tools like dlt.

        Args:
            base: Dataset identifier provided by the replication configuration.
            user: Optional username for rendering user-specific suffixes.

        Returns:
            str: Dataset name including the configured suffix template when present.
        """

        if not self.dataset_suffix_template:
            return base

        if user:
            return f"{base}{self.dataset_suffix_template.format(user=user)}"
        return base

    def state_path(self, root: str) -> str:
        """Return the file-system location used for dbt state artifacts.

        Args:
            root: Base path configured by the caller (usually ``state/``).

        Returns:
            str: State directory, optionally namespaced by ``state_subdir``.
        """

        if self.state_subdir:
            return os.path.join(root, self.state_subdir)
        return root


_ENVIRONMENTS: Mapping[str, EnvironmentConfig] = {
    cfg.name: cfg
    for cfg in (
        EnvironmentConfig(
            name="lab",
            dbt_target="lab",
            database_prefix="_lab_",
            schema_suffix_template="__{user}",
            dataset_suffix_template="__{user}",
            state_subdir="lab",
            keyvault_env_file=".env.lab",
        ),
        EnvironmentConfig(
            name="dev",
            dbt_target="dev",
            database_prefix="_dev_",
            schema_suffix_template="__{user}",
            dataset_suffix_template="__{user}",
            state_subdir="dev",
            keyvault_env_file=".env.dev",
        ),
        EnvironmentConfig(
            name="qa",
            dbt_target="qa",
            database_prefix="_qa_",
            schema_suffix_template=None,
            dataset_suffix_template=None,
            state_subdir="qa",
            keyvault_env_file=".env.qa",
        ),
        EnvironmentConfig(
            name="staging",
            dbt_target="staging",
            database_prefix="_staging_",
            schema_suffix_template=None,
            dataset_suffix_template=None,
            state_subdir="staging",
            keyvault_env_file=".env.staging",
        ),
        EnvironmentConfig(
            name="prod",
            dbt_target="prod",
            database_prefix="",
            schema_suffix_template=None,
            dataset_suffix_template=None,
            state_subdir=None,
            keyvault_env_file=".env",
        ),
    )
}


def get_environment(name: str) -> EnvironmentConfig:
    """Return configuration for the requested environment.

    Args:
        name: Canonical environment name such as ``dev`` or ``prod``.

    Returns:
        EnvironmentConfig: Configuration object describing the named environment.

    Raises:
        KeyError: If ``name`` does not match a configured environment.
    """

    normalized = name.lower()
    if normalized not in _ENVIRONMENTS:
        raise KeyError(f"Unknown environment '{name}'.")
    return _ENVIRONMENTS[normalized]


@lru_cache(maxsize=1)
def get_current_environment() -> EnvironmentConfig:
    """Return configuration for the environment identified by ``TARGET``.

    Returns:
        EnvironmentConfig: Configuration for the environment referenced by the
        ``TARGET`` environment variable.

    Raises:
        KeyError: If ``TARGET`` is set to a value outside ``ENVIRONMENT_SEQUENCE``.
    """

    target = os.getenv("TARGET", "dev").lower()
    try:
        return get_environment(target)
    except KeyError as error:
        raise KeyError(
            "Unsupported TARGET environment. Set TARGET to one of: "
            + ", ".join(ENVIRONMENT_SEQUENCE)
        ) from error


def iter_environments() -> Iterable[EnvironmentConfig]:
    """Iterate over known environment configurations in precedence order.

    Returns:
        Iterable[EnvironmentConfig]: Iterator yielding configurations ordered by
        ``ENVIRONMENT_SEQUENCE``.
    """

    for name in ENVIRONMENT_SEQUENCE:
        yield get_environment(name)
