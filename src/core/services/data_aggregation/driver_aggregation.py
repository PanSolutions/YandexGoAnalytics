from __future__ import annotations

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.core.services.data_aggregation.base_aggregation import BaseAggregationService


class DriverGoldMetricsService(BaseAggregationService):
    def __init__(self) -> None:
        super().__init__(source_table_name="drivers", target_table_name="driver_metrics")

    def optimize(self, spark: SparkSession) -> None:
        logger.info(f"Running Z-Order on {self.full_target_table}")
        spark.sql(f"OPTIMIZE {self.full_target_table} ZORDER BY (experience)")

    def transform_stream(self, df: DataFrame) -> DataFrame:
        return (
            df.groupBy("experience")
            .agg(
                F.count("*").alias("drivers_count"),
                F.round(F.avg("rating"), 2).alias("avg_rating"),
            )
            .withColumn("_calculated_at", F.current_timestamp())
        )
