"""Integration test fixtures providing an authentic remote Databricks SparkSession."""

from __future__ import annotations

from typing import Generator

import pytest
from pyspark.sql import SparkSession

from src.core.infra.connection.databricks_connector import DatabricksConnectionService


@pytest.fixture(scope="function")
def spark_integration_session() -> Generator[SparkSession, None, None]:
    """Provide a dedicated remote SparkSession for each test function.

    Guarantees an open gRPC channel without premature teardown between tests.

    Yields:
        SparkSession: Real remote Databricks SparkSession.
    """
    service = DatabricksConnectionService(app_name="integration-tests")
    service.connect()
    spark: SparkSession = service.spark

    try:
        if hasattr(spark, "asDefault"):
            spark.asDefault()
        yield spark
    finally:
        service.disconnect()
