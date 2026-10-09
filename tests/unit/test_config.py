from __future__ import annotations

import os
from unittest import mock

import pytest

from src.core.config.settings import get_catalog, get_environment


class TestConfigurationService:
    """Test suite verifying application configuration and environment resolution."""

    def test_get_catalog_success(self) -> None:
        """Ensure get_catalog returns the set environment variable."""
        with mock.patch.dict(os.environ, {"DATABRICKS_CATALOG": "test_catalog"}):
            assert get_catalog() == "test_catalog"

    def test_get_catalog_missing_raises_error(self) -> None:
        """Ensure get_catalog raises RuntimeError when environment variable is unset."""
        with mock.patch.dict(os.environ, {}, clear=True):
            with pytest.raises(RuntimeError, match="DATABRICKS_CATALOG is not set"):
                get_catalog()

    def test_get_environment_success(self) -> None:
        """Ensure get_environment returns the deployment environment."""
        with mock.patch.dict(os.environ, {"DATABRICKS_ENV": "staging"}):
            assert get_environment() == "staging"

    def test_get_environment_missing_raises_error(self) -> None:
        """Ensure get_environment raises RuntimeError when variable is unset."""
        with mock.patch.dict(os.environ, {}, clear=True):
            with pytest.raises(RuntimeError, match="DATABRICKS_ENV is not set"):
                get_environment()
