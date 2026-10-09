from __future__ import annotations

import os

from loguru import logger
from pyspark.sql import SparkSession

from .base_connector import BaseConnectionService


class DatabricksConnectionError(RuntimeError):
    """Raised when SparkSession cannot be established."""


class DatabricksConnectionService(BaseConnectionService):
    """Provide a ``SparkSession`` inside or outside the Databricks runtime.

    Inside the Databricks runtime the active session is reused. Elsewhere
    a serverless Databricks Connect session is created. Use as a context manager:

    .. code-block:: python

        with DatabricksConnectionService("my-app") as conn:
            conn.spark.sql("SELECT 1")
    """

    def __init__(self, app_name: str) -> None:
        """Initialize the service.

        Args:
            app_name: Application name, used for logging.
        """
        super().__init__()
        self._app_name = app_name
        self._spark: SparkSession | None = None

    @property
    def spark(self) -> SparkSession:
        """Return the active ``SparkSession``.

        Raises:
            DatabricksConnectionError: If the service is not connected.
        """
        if not self.is_connected or self._spark is None:
            raise DatabricksConnectionError(
                "DatabricksConnectionService is not connected. "
                "Use it as a context manager: "
                "`with DatabricksConnectionService(...) as conn:`"
            )
        return self._spark

    def connect(self) -> None:
        """Create or reuse a ``SparkSession`` and mark the service as connected."""
        self._spark = self._build_spark()
        self.is_connected = True
        logger.info(
            f"SparkSession ready | app={self._app_name} | "
            f"version={self._spark.version} | "
            f"runtime={'databricks' if self._is_runtime() else 'serverless-connect'}"
        )

    def disconnect(self) -> None:
        """Stop the session if it is a Databricks Connect one and drop the reference.

        Sessions of the native Databricks runtime are left running.
        """
        if self._spark is not None:
            if self._is_runtime():
                logger.info("ℹ️ Databricks runtime detected — skip spark.stop()")
            else:
                logger.info("🛑 Stopping Serverless Databricks Connect session")
                self._spark.stop()

            self._spark = None

        self.is_connected = False

    @staticmethod
    def _is_runtime() -> bool:
        """Return ``True`` when running inside the Databricks runtime."""
        return "DATABRICKS_RUNTIME_VERSION" in os.environ

    def _build_spark(self) -> SparkSession:
        """Build a ``SparkSession`` for the current environment.

        Raises:
            DatabricksConnectionError: If no active session exists in the runtime,
                ``databricks-connect`` is missing, or the connection fails.
        """
        # 1 Native Databricks Runtime
        if self._is_runtime():
            active = SparkSession.getActiveSession()
            if active is None:
                raise DatabricksConnectionError(
                    "Running inside Databricks Runtime, but no active SparkSession found."
                )
            return active

        # 2 Databricks Connect (Serverless OAuth)
        try:
            from databricks.connect import DatabricksSession
        except ImportError as e:
            raise DatabricksConnectionError(
                "`databricks-connect` is not installed. Run: `uv add 'databricks-connect==15.4.*'`"
            ) from e

        try:
            return DatabricksSession.builder.serverless().getOrCreate()
        except Exception as e:
            raise DatabricksConnectionError(f"Failed to connect: {e}") from e
