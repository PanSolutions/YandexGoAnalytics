from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()


def get_catalog() -> str:
    """Return the Unity Catalog name from ``DATABRICKS_CATALOG``.

    Returns:
        The catalog name.

    Raises:
        RuntimeError: If the variable is not set or empty.
    """
    catalog = os.environ.get("DATABRICKS_CATALOG")
    if not catalog:
        raise RuntimeError("Environment variable DATABRICKS_CATALOG is not set")
    return catalog


def get_environment() -> str:
    """Return the deployment environment name from ``DATABRICKS_ENV``.

    Returns:
        The environment name (e.g. ``dev``, ``staging``, ``prod``).

    Raises:
        RuntimeError: If the variable is not set or empty.
    """
    env = os.environ.get("DATABRICKS_ENV")
    if not env:
        raise RuntimeError("Environment variable DATABRICKS_ENV is not set")
    return env
