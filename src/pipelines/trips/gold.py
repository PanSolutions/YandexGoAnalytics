from __future__ import annotations

from pyspark.sql import DataFrame

try:
    import dlt
except ImportError:
    from unittest.mock import MagicMock

    dlt = MagicMock()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


class TripGoldMetricsPipeline:
    SOURCE_TABLE = "silver_taxi"
    TARGET_TABLE = "gold_taxi_metrics"

    @classmethod
    def aggregate(cls, df: DataFrame) -> DataFrame:
        return (
            df.groupBy("pu_location_id", "payment_type_description")
            .agg(
                F.count("*").alias("total_trips"),
                F.round(F.sum("fare_amount"), 2).alias("total_revenue"),
            )
            .withColumn("_calculated_at", F.current_timestamp())
            .orderBy(F.col("total_revenue").desc())
        )


class DriverGoldMetricsPipeline:
    SOURCE_TABLE = "silver_drivers"
    TARGET_TABLE = "gold_driver_metrics"

    @classmethod
    def aggregate(cls, df: DataFrame) -> DataFrame:
        return (
            df.groupBy("experience")
            .agg(
                F.count("*").alias("drivers_count"),
                F.round(F.avg("rating"), 2).alias("avg_rating"),
            )
            .withColumn("_calculated_at", F.current_timestamp())
            .orderBy("experience")
        )


@dlt.table(
    name=DriverGoldMetricsPipeline.TARGET_TABLE,
    comment="Driver distribution and average rating aggregated by experience",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "experience",
    },
)
def gold_driver_metrics():
    return DriverGoldMetricsPipeline.aggregate(
        dlt.read(DriverGoldMetricsPipeline.SOURCE_TABLE)
    )


class UserGoldMetricsPipeline:
    SOURCE_TABLE = "silver_users"
    TARGET_TABLE = "gold_user_metrics"

    @classmethod
    def aggregate(cls, df: DataFrame) -> DataFrame:
        return (
            df.groupBy("registration_date")
            .agg(F.count("*").alias("users_count"))
            .withColumn("_calculated_at", F.current_timestamp())
            .orderBy(F.col("registration_date").desc())
        )


class TripEnrichmentService:
    @staticmethod
    def enrich(trips_df: DataFrame, drivers_df: DataFrame) -> DataFrame:
        clean_drivers = drivers_df.select(
            F.col("id").alias("driver_id"),
            F.col("name").alias("driver_name"),
            F.col("car_number").alias("driver_car_number"),
            F.col("experience").alias("driver_experience"),
            F.col("rating").alias("driver_rating"),
        )

        return trips_df.join(clean_drivers, on="driver_id", how="left").withColumn(
            "_calculated_at", F.current_timestamp()
        )


@dlt.table(
    name=TripGoldMetricsPipeline.TARGET_TABLE,
    comment="Total revenue and trip volume aggregated by pickup location",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.clusteringColumns": "pu_location_id",
    },
)
def gold_taxi_metrics():
    return TripGoldMetricsPipeline.aggregate(
        dlt.read(TripGoldMetricsPipeline.SOURCE_TABLE)
    )


@dlt.table(name="gold_enriched_trips")
def gold_enriched_trips():
    return TripEnrichmentService.enrich(
        trips_df=dlt.read_stream("silver_taxi"),
        drivers_df=dlt.read("silver_drivers"),
    )


@dlt.table(
    name=UserGoldMetricsPipeline.TARGET_TABLE,
    comment="User registrations aggregated by date",
)
def gold_user_metrics():
    return UserGoldMetricsPipeline.aggregate(
        dlt.read(UserGoldMetricsPipeline.SOURCE_TABLE)
    )
