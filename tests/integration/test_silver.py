"""Integration tests for Silver layer data transformations and cleansing."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import patch

from pyspark.sql import DataFrame, Row, SparkSession

from src.core.etl.silver.driver import DriverSilverTransformationService
from src.core.etl.silver.trip import TripSilverTransformationService
from src.core.etl.silver.user import UserSilverTransformationService


class TestSilverTransformations:
    """Integration test suite executing actual Spark transformations on Silver layer."""

    @patch("src.core.etl.silver.base.get_catalog", return_value="test_cat")
    def test_driver_silver_transformation(
        self, _: object, spark_integration_session: SparkSession
    ) -> None:
        """Verify Driver cleansing rules: experience > 0, rating [1..5], deduplication."""
        test_data = [
            Row(id="D1", name="Ivan", experience=5, rating=4.8),
            Row(id="D2", name="Petr", experience=0, rating=4.5),
            Row(id="D3", name="Anna", experience=3, rating=5.5),
            Row(id="D4", name="Oleg", experience=2, rating=0.8),
            Row(id="D1", name="Ivan Duplicate", experience=5, rating=4.8),
        ]
        input_df: DataFrame = spark_integration_session.createDataFrame(test_data)

        service = DriverSilverTransformationService()
        result_df = service.transform(input_df)

        rows = result_df.collect()
        assert len(rows) == 1
        assert rows[0]["id"] == "D1"
        assert "_processed_at" in result_df.columns

    @patch("src.core.etl.silver.base.get_catalog", return_value="test_cat")
    def test_trip_silver_transformation(
        self, _: object, spark_integration_session: SparkSession
    ) -> None:
        """Verify Trip cleansing rules: positive fields, payment descriptions, business keys deduplication."""
        now = datetime(2026, 10, 1, 12, 0, 0)
        test_data = [
            Row(
                user_id="U1",
                driver_id="D1",
                pickup_datetime=now,
                dropoff_datetime=now,
                passenger_count=1,
                trip_distance=5.0,
                fare_amount=15.0,
                payment_type=1,
            ),
            Row(
                user_id="U2",
                driver_id="D2",
                pickup_datetime=now,
                dropoff_datetime=now,
                passenger_count=2,
                trip_distance=3.0,
                fare_amount=10.0,
                payment_type=2,
            ),
            Row(
                user_id="U3",
                driver_id="D3",
                pickup_datetime=now,
                dropoff_datetime=now,
                passenger_count=1,
                trip_distance=2.0,
                fare_amount=8.0,
                payment_type=99,
            ),
            Row(
                user_id="U4",
                driver_id="D4",
                pickup_datetime=now,
                dropoff_datetime=now,
                passenger_count=0,
                trip_distance=5.0,
                fare_amount=15.0,
                payment_type=1,
            ),
            Row(
                user_id="U1",
                driver_id="D1",
                pickup_datetime=now,
                dropoff_datetime=now,
                passenger_count=1,
                trip_distance=5.0,
                fare_amount=15.0,
                payment_type=1,
            ),
        ]
        input_df = spark_integration_session.createDataFrame(test_data)

        service = TripSilverTransformationService()
        result_df = service.transform(input_df)

        assert result_df.count() == 3

        res_dict = {
            r["user_id"]: r["payment_type_description"] for r in result_df.collect()
        }
        assert res_dict["U1"] == "Credit Card"
        assert res_dict["U2"] == "Cash"
        assert res_dict["U3"] == "Other"

    @patch("src.core.etl.silver.base.get_catalog", return_value="test_cat")
    def test_user_silver_transformation(
        self, _: object, spark_integration_session: SparkSession
    ) -> None:
        """Verify User cleansing rules: drop null name, coalesce null email, deduplicate email."""
        test_data = [
            Row(id="1", full_name="Alice", email="alice@test.com"),
            Row(id="2", full_name=None, email="noname@test.com"),
            Row(id="3", full_name="Bob", email=None),
            Row(id="4", full_name="Charlie", email=None),
        ]
        input_df = spark_integration_session.createDataFrame(test_data)

        service = UserSilverTransformationService()
        result_df = service.transform(input_df)

        rows = result_df.collect()
        assert len(rows) == 2
        emails = [r["email"] for r in rows]
        assert "alice@test.com" in emails
        assert "Unknown" in emails
