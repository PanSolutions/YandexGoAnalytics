"""Spark session fixtures for unit and integration testing suites."""

from __future__ import annotations

from typing import Generator
from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame, SparkSession

from src.core.infra.connection.databricks_connector import DatabricksConnectionService


@pytest.fixture(scope="session")
def spark_mock_session() -> MagicMock:
    """Provide a fast SparkSession mock for isolated unit tests.

    Returns:
        MagicMock: Mocked SparkSession instance.
    """
    mock = MagicMock(spec=SparkSession)
    mock.version = "15.4"
    mock.catalog = MagicMock()
    mock.catalog.tableExists.return_value = True

    df_mock = MagicMock(spec=DataFrame)
    mock.createDataFrame.return_value = df_mock
    return mock


@pytest.fixture(scope="session")
def spark_integration_session() -> Generator[SparkSession, None, None]:
    """Provide a persistent remote SparkSession for integration tests.

    Initializes connection to Databricks Serverless once per test session
    to minimize handshake latency across test cases.

    Yields:
        SparkSession: Authenticated remote Databricks SparkSession.
    """
    service = DatabricksConnectionService(app_name="pytest-integration-suite")
    service.connect()
    spark: SparkSession = service.spark

    if hasattr(spark, "asDefault"):
        spark.asDefault()

    try:
        yield spark
    finally:
        service.disconnect()
