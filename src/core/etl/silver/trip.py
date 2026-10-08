from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .base import BaseTransformationService


class TripSilverTransformationService(BaseTransformationService):
    """
    - passenger_count > 0, trip_distance > 0, fare_amount > 0
    - mapping payment_type (1 = Credit Card, 2 = Cash, otherwise = Other)
    - add processed_time
    - drop duplicate records
    """

    def __init__(self) -> None:
        super().__init__(table_name="taxi")

    def transform(self, df: DataFrame) -> DataFrame:

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
