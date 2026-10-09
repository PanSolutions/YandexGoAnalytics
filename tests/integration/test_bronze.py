"""Integration tests for Bronze layer ingestion services."""

from __future__ import annotations

from unittest.mock import patch

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from src.core.etl.bronze.base import BaseIngestionService
from src.core.etl.bronze.driver import DriverStreamingIngestionService
from src.core.etl.bronze.trip import TaxiTripBatchIngestionService
from src.core.etl.bronze.user import UserBatchIngestionService


class TestBronzeIngestionServices:
    """Integration test suite for bronze layer ingestion components."""

    @patch("src.core.etl.bronze.base.get_catalog", return_value="test_cat")
    def test_base_ingestion_path_resolution(self, _: object) -> None:
        """Check proper Unity Catalog paths formatting across Bronze services."""
        user_service = UserBatchIngestionService()
        assert user_service.source_format == "avro"
        assert (
            user_service.source_path == "/Volumes/test_cat/raw_files/landing/avro"
        )
        assert (
            user_service.checkpoint_path
            == "/Volumes/test_cat/raw_files/landing/_checkpoints/bronze/users"
        )
        assert user_service.full_target_table == "test_cat.bronze.users"

        trip_service = TaxiTripBatchIngestionService()
        assert trip_service.source_format == "parquet"
        assert trip_service.full_target_table == "test_cat.bronze.taxi"

    @patch("src.core.etl.bronze.base.get_catalog", return_value="test_cat")
    def test_add_audit_metadata(
        self, _: object, spark_integration_session: SparkSession
    ) -> None:
        """Ensure technical metadata columns are appended to the DataFrame."""
        source_data = [
            Row(id="1", file_path="dbfs:/landing/file1.json"),
            Row(id="2", file_path="dbfs:/landing/file2.json"),
        ]
        # Эмулируем структуру `_metadata` со столбцом `file_path`
        input_df: DataFrame = spark_integration_session.createDataFrame(source_data).select(
            "id",
            F.struct(F.col("file_path")).alias("_metadata"),
        )

        transformed_df = BaseIngestionService.add_audit_metadata(input_df)

        assert "_ingested_at" in transformed_df.columns
        assert "_source_file" in transformed_df.columns

        first_row = transformed_df.filter(transformed_df.id == "1").first()
        assert first_row is not None
        assert first_row["_source_file"] == "dbfs:/landing/file1.json"
        assert first_row["_ingested_at"] is not None

    @patch("src.core.etl.bronze.base.get_catalog", return_value="test_cat")
    def test_driver_streaming_ingestion_init(self, _: object) -> None:
        """Verify driver ingestion service schema hints configuration."""
        driver_service = DriverStreamingIngestionService(include_phone_in_base_schema=True)
        assert driver_service.include_phone_in_base_schema is True
        assert driver_service.full_target_table == "test_cat.bronze.drivers"
