from __future__ import annotations

from pathlib import Path
import os

from loguru import logger
from pyspark.sql import SparkSession

from src.core.services.connection.base_connector import BaseConnectionService

from dotenv import find_dotenv, load_dotenv


class DatabricksConnectionError(RuntimeError):
    """Raised when SparkSession cannot be established."""


class DatabricksConnectionService(BaseConnectionService):

    def __init__(
            self,
            app_name: str,
            cluster_id: str | None = None,
    ) -> None:
        super().__init__()
        self._ensure_env_loaded()

        self._app_name = app_name
        self._cluster_id = cluster_id or os.environ.get("DATABRICKS_CLUSTER_ID")
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
        super().connect()
        self._spark = self._build_spark()
        logger.info(
            f"SparkSession ready | app={self._app_name} | "
            f"version={self._spark.version} | "
            f"runtime={'databricks' if self._is_runtime() else 'connect'}"
        )

    def disconnect(self) -> None:
        if self._spark is not None:
            if self._is_runtime():
                logger.info("ℹ️ Databricks runtime detected — skip spark.stop()")
            else:
                logger.info("🛑 Stopping Databricks Connect session")
                self._spark.stop()

            self._spark = None

        super().disconnect()

    @staticmethod
    def _is_runtime() -> bool:
        return "DATABRICKS_RUNTIME_VERSION" in os.environ

    @classmethod
    def _ensure_env_loaded(cls) -> None:
        if cls._is_runtime():
            return

        try:
            dotenv_path = find_dotenv(usecwd=True)
            if dotenv_path:
                load_dotenv(dotenv_path)
                return
        except ImportError:
            pass

        current = Path.cwd().resolve()
        for parent in [current] + list(current.parents):
            env_file = parent / ".env"
            if env_file.is_file():
                with open(env_file, encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#") and "=" in line:
                            k, _, v = line.partition("=")
                            os.environ.setdefault(k.strip(), v.strip().strip("\"'"))
                return

    def _build_spark(self) -> SparkSession:
        if self._is_runtime():
            active = SparkSession.getActiveSession()
            if active is None:
                raise DatabricksConnectionError(
                    "Running inside Databricks Runtime, but no active SparkSession."
                )
            return active

        try:
            from databricks.connect import DatabricksSession
        except ImportError as e:
            raise DatabricksConnectionError(
                "`databricks-connect` is not installed. "
                "Run: `uv add 'databricks-connect==15.4.*'`"
            ) from e

        host = os.environ.get("DATABRICKS_HOST")
        token = os.environ.get("DATABRICKS_TOKEN")

        if not host:
            raise DatabricksConnectionError(
                "Missing 'DATABRICKS_HOST'. Check that your .env file exists and contains it."
            )
        if not token:
            raise DatabricksConnectionError(
                "Missing 'DATABRICKS_TOKEN'. Check that your .env file exists and contains it."
            )

        builder = DatabricksSession.builder.host(host.strip()).token(token.strip())

        if self._cluster_id:
            logger.info(f"🔗 Connecting via Databricks Connect to cluster {self._cluster_id}")
            return builder.clusterId(self._cluster_id.strip()).getOrCreate()

        logger.info("☁️ No cluster_id — connecting to Serverless compute")
        session = builder.serverless().getOrCreate()
        logger.info("🔗 Connected via Databricks Connect to Serverless")
        return session
