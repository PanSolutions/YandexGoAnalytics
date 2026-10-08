from __future__ import annotations

import os

from loguru import logger
from pyspark.sql import SparkSession

from .base_connector import BaseConnectionService


class DatabricksConnectionError(RuntimeError):
    """Raised when SparkSession cannot be established."""


class DatabricksConnectionService(BaseConnectionService):
    def __init__(self, app_name) -> None:
        super().__init__()
        self._app_name = app_name
        self._spark: SparkSession | None = None

    @property
    def spark(self) -> SparkSession:
        if not self.is_connected or self._spark is None:
            raise DatabricksConnectionError(
                "DatabricksConnectionService is not connected. "
                "Use it as a context manager: "
                "`with DatabricksConnectionService(...) as conn:`"
            )
        return self._spark

    def connect(self) -> None:
        self._spark = self._build_spark()
        self.is_connected = True
        logger.info(
            f"SparkSession ready | app={self._app_name} | "
            f"version={self._spark.version} | "
            f"runtime={'databricks' if self._is_runtime() else 'serverless-connect'}"
        )

    def disconnect(self) -> None:
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
        return "DATABRICKS_RUNTIME_VERSION" in os.environ

    def _build_spark(self) -> SparkSession:
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
