from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()


def get_catalog() -> str:
    catalog = os.environ.get("DATABRICKS_CATALOG")
    if not catalog:
        raise RuntimeError("Environment variable DATABRICKS_CATALOG is not set")
    return catalog


def get_environment() -> str:
    env = os.environ.get("DATABRICKS_ENV")
    if not env:
        raise RuntimeError("Environment variable DATABRICKS_CATALOG is not set")
    return env
