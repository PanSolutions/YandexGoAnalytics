from __future__ import annotations

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.core.services.data_aggregation.base_aggregation import BaseAggregationService


class TripGoldMetricsService(BaseAggregationService):
    def __init__(self) -> None:
        super().__init__(source_table_name="taxi", target_table_name="taxi_metrics")

    def optimize(self, spark: SparkSession) -> None:
        logger.info(
            f"Enabling Liquid Clustering on [pu_location_id] and optimizing {self.full_target_table}..."
        )
        spark.sql(f"ALTER TABLE {self.full_target_table} CLUSTER BY (pu_location_id)")
        spark.sql(f"OPTIMIZE {self.full_target_table}")

    def transform_stream(self, df: DataFrame) -> DataFrame:
        return (
            df.groupBy("pu_location_id", "payment_type_description")
            .agg(
                F.count("*").alias("total_trips"),
                F.round(F.sum("fare_amount"), 2).alias("total_revenue"),
            )
            .withColumn("_calculated_at", F.current_timestamp())
        )
