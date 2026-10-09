"""Integration tests for Gold layer metric aggregations."""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

from pyspark.sql import DataFrame, Row, SparkSession

from src.core.etl.gold.driver import DriverGoldMetricsService
from src.core.etl.gold.trip import TripGoldMetricsService
from src.core.etl.gold.user import UserGoldMetricsService


class TestGoldAggregations:
    """Integration test suite executing metric aggregation calculations."""

    @patch("src.core.etl.gold.base.get_catalog", return_value="test_cat")
    def test_driver_gold_metrics_aggregation(
        self, _: object, spark_integration_session: SparkSession
    ) -> None:
        """Validate driver count and rounded avg rating grouped by experience."""
        test_data = [
            Row(experience=3, rating=4.5),
            Row(experience=3, rating=4.7),
            Row(experience=5, rating=5.0),
        ]
        df: DataFrame = spark_integration_session.createDataFrame(test_data)

        service = DriverGoldMetricsService()
        result_df = service.transform_stream(df)

        metrics = {r["experience"]: r for r in result_df.collect()}
        assert metrics[3]["drivers_count"] == 2
        assert metrics[3]["avg_rating"] == 4.6
        assert metrics[5]["drivers_count"] == 1
        assert metrics[5]["avg_rating"] == 5.0
        assert "_calculated_at" in result_df.columns

    @patch("src.core.etl.gold.base.get_catalog", return_value="test_cat")
    def test_trip_gold_metrics_aggregation(
        self, _: object, spark_integration_session: SparkSession
    ) -> None:
        """Validate trip aggregation by pickup zone and payment type."""
        test_data = [
            Row(
                pu_location_id=10,
                payment_type_description="Cash",
                fare_amount=20.50,
            ),
            Row(
                pu_location_id=10,
                payment_type_description="Cash",
                fare_amount=15.25,
            ),
            Row(
                pu_location_id=10,
                payment_type_description="Credit Card",
                fare_amount=30.00,
            ),
        ]
        df = spark_integration_session.createDataFrame(test_data)

        service = TripGoldMetricsService()
        result_df = service.transform_stream(df)

        rows = result_df.collect()
        assert len(rows) == 2

        cash_group = next(
            r for r in rows if r["payment_type_description"] == "Cash"
        )
        assert cash_group["total_trips"] == 2
        assert cash_group["total_revenue"] == 35.75

    @patch("src.core.etl.gold.base.get_catalog", return_value="test_cat")
    def test_user_gold_metrics_aggregation(
        self, _: object, spark_integration_session: SparkSession
    ) -> None:
        """Validate user registration counts per date."""
        d1 = date(2026, 10, 1)
        d2 = date(2026, 10, 2)
        test_data = [
            Row(registration_date=d1),
            Row(registration_date=d1),
            Row(registration_date=d2),
        ]
        df = spark_integration_session.createDataFrame(test_data)

        service = UserGoldMetricsService()
        result_df = service.transform_stream(df)

        metrics = {r["registration_date"]: r["users_count"] for r in result_df.collect()}
        assert metrics[d1] == 2
        assert metrics[d2] == 1
