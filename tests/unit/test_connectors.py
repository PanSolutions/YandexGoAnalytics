from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.core.infra.connection.base_connector import BaseConnectionService
from src.core.infra.connection.databricks_connector import (
    DatabricksConnectionError,
    DatabricksConnectionService,
)


class DummyConnectionService(BaseConnectionService):
    """Concrete implementation of BaseConnectionService for testing purposes."""

    def __init__(self) -> None:
        super().__init__()
        self.connect_called: bool = False
        self.disconnect_called: bool = False

    def connect(self) -> None:
        """Simulate connection establishment."""
        self.connect_called = True

    def disconnect(self) -> None:
        """Simulate connection tear down."""
        self.disconnect_called = True


class TestBaseConnectionService:
    """Test suite verifying context manager protocol for BaseConnectionService."""

    def test_context_manager_lifecycle(self) -> None:
        """Ensure context manager properly calls connect and disconnect."""
        service = DummyConnectionService()
        assert not service.is_connected

        with service as conn:
            assert conn.is_connected
            assert conn.connect_called

        assert not service.is_connected
        assert service.disconnect_called

    def test_context_manager_propagates_exception(self) -> None:
        """Verify exceptions inside the context block are not suppressed."""
        service = DummyConnectionService()
        with pytest.raises(ValueError, match="Boom"):
            with service:
                raise ValueError("Boom")
        assert not service.is_connected
        assert service.disconnect_called


class TestDatabricksConnectionService:
    """Test suite validating Databricks SparkSession initialization logic."""

    def test_unconnected_spark_access_raises_error(self) -> None:
        """Accessing .spark prior to connection should raise DatabricksConnectionError."""
        service = DatabricksConnectionService(app_name="unit-test")
        with pytest.raises(DatabricksConnectionError, match="is not connected"):
            _ = service.spark

    @patch.dict("os.environ", {"DATABRICKS_RUNTIME_VERSION": "15.4"})
    @patch("pyspark.sql.SparkSession.getActiveSession")
    def test_runtime_connection_success(self, mock_active_session: MagicMock) -> None:
        """Ensure connection attaches to active session inside Databricks runtime."""
        mock_spark = MagicMock()
        mock_spark.version = "15.4"
        mock_active_session.return_value = mock_spark

        service = DatabricksConnectionService(app_name="runtime-test")
        service.connect()

        assert service.is_connected
        assert service.spark == mock_spark

        service.disconnect()
        assert not service.is_connected
        mock_spark.stop.assert_not_called()

    @patch.dict("os.environ", {}, clear=True)
    def test_missing_databricks_connect_raises_error(self) -> None:
        """Ensure clear exception if databricks-connect is missing on external host."""
        service = DatabricksConnectionService(app_name="external-test")
        with patch.dict("sys.modules", {"databricks.connect": None}):
            with pytest.raises(
                DatabricksConnectionError, match="`databricks-connect` is not installed"
            ):
                service.connect()
