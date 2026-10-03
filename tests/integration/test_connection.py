import pytest

from src.core.services.connection.databricks_connector import (
    DatabricksConnectionService,
)


@pytest.fixture(scope="module")
def spark():
    conn = DatabricksConnectionService(app_name="ci-integration-check")
    conn.connect()

    yield conn.spark

    conn.disconnect()


def test_databricks_spark_connectivity(spark):
    result = spark.sql("SELECT 1 AS ping").collect()

    assert len(result) == 1
    assert result[0]["ping"] == 1
