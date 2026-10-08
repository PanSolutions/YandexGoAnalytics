from __future__ import annotations

from typing import ClassVar

try:
    import dlt
except ImportError:
    from unittest.mock import MagicMock

    dlt = MagicMock()

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


class TripSilverPipeline:
    SOURCE_TABLE = "bronze_taxi"
    TARGET_TABLE = "silver_taxi"

    DATA_QUALITY_RULES: ClassVar[dict[str, str]] = {
        "valid_passenger_count": "passenger_count > 0",
        "valid_trip_distance": "trip_distance > 0",
        "valid_fare_amount": "fare_amount > 0",
    }

    DEDUP_COLUMNS: ClassVar[list[str]] = [
        "user_id",
        "driver_id",
        "pickup_datetime",
        "dropoff_datetime",
    ]

    @classmethod
    def clean(cls, df: DataFrame) -> DataFrame:
        return (
            df.withColumn(
                "payment_type_description",
                F.when(F.col("payment_type") == 1, "Credit Card")
                .when(F.col("payment_type") == 2, "Cash")
                .otherwise("Other"),
            )
            .withColumn("_processed_at", F.current_timestamp())
            .dropDuplicates(subset=cls.DEDUP_COLUMNS)
        )


class DriverSilverPipeline:
    SOURCE_TABLE = "bronze_drivers"
    TARGET_TABLE = "silver_drivers"

    DATA_QUALITY_RULES: ClassVar[dict[str, str]] = {
        "valid_experience": "experience > 0",
        "valid_rating_range": "rating >= 1.0 AND rating <= 5.0",
    }

    @classmethod
    def clean(cls, df: DataFrame) -> DataFrame:
        return df.withColumn("_processed_at", F.current_timestamp()).dropDuplicates(["id"])


class UserSilverPipeline:
    SOURCE_TABLE = "bronze_users"
    TARGET_TABLE = "silver_users"

    DATA_QUALITY_RULES: ClassVar[dict[str, str]] = {
        "valid_full_name": "full_name IS NOT NULL",
    }

    @classmethod
    def clean(cls, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("email", F.coalesce(F.col("email"), F.lit("Unknown")))
            .withColumn("_processed_at", F.current_timestamp())
            .dropDuplicates(subset=["email"])
        )


@dlt.table(name=TripSilverPipeline.TARGET_TABLE)
@dlt.expect_all_or_drop(TripSilverPipeline.DATA_QUALITY_RULES)
def silver_taxi():
    return TripSilverPipeline.clean(dlt.read_stream(TripSilverPipeline.SOURCE_TABLE))


@dlt.table(name=DriverSilverPipeline.TARGET_TABLE)
@dlt.expect_all_or_drop(DriverSilverPipeline.DATA_QUALITY_RULES)
def silver_drivers():
    return DriverSilverPipeline.clean(dlt.read_stream(DriverSilverPipeline.SOURCE_TABLE))


@dlt.table(name=UserSilverPipeline.TARGET_TABLE)
@dlt.expect_all_or_drop(UserSilverPipeline.DATA_QUALITY_RULES)
def silver_users():
    return UserSilverPipeline.clean(dlt.read_stream(UserSilverPipeline.SOURCE_TABLE))
