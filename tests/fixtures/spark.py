"""Pytest fixtures for simulating SparkSession in environments with databricks-connect."""

from __future__ import annotations

from typing import Generator
from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame, SparkSession


@pytest.fixture(scope="session")
def spark_session() -> Generator[SparkSession, None, None]:
    """Provide a mock SparkSession fixture compatible with databricks-connect.

    Yields:
        SparkSession: Mocked SparkSession instance supporting DataFrame creation.
    """
    mock_spark = MagicMock(spec=SparkSession)
    mock_spark.version = "15.4"

    def _mock_create_df(data: list[object], *args: object, **kwargs: object) -> MagicMock:
        mock_df = MagicMock(spec=DataFrame)
        mock_df.count.return_value = len(data)
        mock_df.collect.return_value = data
        mock_df.first.return_value = data[0] if data else None

        if data and hasattr(data[0], "__dict__"):
            mock_df.columns = list(data[0].__dict__.keys())
        elif data and hasattr(data[0], "asDict"):
            mock_df.columns = list(data[0].asDict().keys())
        else:
            mock_df.columns = ["id", "phone", "rating"]

        mock_df.filter.return_value = mock_df
        return mock_df

    mock_spark.createDataFrame.side_effect = _mock_create_df
    mock_spark.catalog = MagicMock()
    mock_spark.catalog.tableExists.return_value = True

    yield mock_spark
