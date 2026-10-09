from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .base import BaseTransformationService


class TripSilverTransformationService(BaseTransformationService):
    """Build ``silver.taxi`` from ``bronze.taxi``.

    Rules:
        - keep rows with ``passenger_count``, ``trip_distance`` and ``fare_amount`` > 0
        - map ``payment_type`` to ``payment_type_description``
          (1 = Credit Card, 2 = Cash, otherwise Other)
        - add ``_processed_at``
        - drop duplicates by ``user_id``, ``driver_id``, ``pickup_datetime``
          and ``dropoff_datetime``
    """

    def __init__(self) -> None:
        """Bind the service to the ``taxi`` table."""
        super().__init__(table_name="taxi")

    def transform(self, df: DataFrame) -> DataFrame:
        """Filter invalid trips, decode the payment type and deduplicate."""

        trip_business_keys = [
            "user_id",
            "driver_id",
            "pickup_datetime",
            "dropoff_datetime",
        ]

        return (
            df.filter(
                (F.col("passenger_count") > 0)
                & (F.col("trip_distance") > 0)
                & (F.col("fare_amount") > 0)
            )
            .withColumn(
                "payment_type_description",
                F.when(F.col("payment_type") == 1, "Credit Card")
                .when(F.col("payment_type") == 2, "Cash")
                .otherwise("Other"),
            )
            .withColumn("_processed_at", F.current_timestamp())
            .dropDuplicates(subset=trip_business_keys)
        )
